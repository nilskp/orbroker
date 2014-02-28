package org.orbroker.util

private[orbroker] class Mirror {

  private val lookups = new java.util.concurrent.ConcurrentHashMap[(Class[_], String), (AnyRef ⇒ Any)]

  private def nodeValue(nodeObj: AnyRef, nodeName: String): Any = {
    if (nodeObj == null) {
      null
    } else {
      val key = (nodeObj.getClass, nodeName)
      var lookup = lookups.get(key)
      if (lookup == null) {
        lookup = makeLookup(nodeObj, nodeName)
        lookups.putIfAbsent(key, lookup)
      }
      lookup(nodeObj)
    }
  }

  @scala.annotation.tailrec
  private def isNumber(str: String, idx: Int = 0): Boolean = {
    if (str.length == 0) {
      false
    } else if (idx == str.length) {
      true
    } else {
      str.charAt(idx) >= '0' && str.charAt(idx) <= '9' && isNumber(str, idx + 1)
    }
  }

  private def makeLookup(parmObj: AnyRef, nodeName: String): (AnyRef ⇒ Any) = {
    if (parmObj.isInstanceOf[java.util.Map[_, _]]) {
      return (obj: AnyRef) ⇒ obj.asInstanceOf[java.util.Map[String, _]].get(nodeName)
    } else if (parmObj.isInstanceOf[scala.collection.Map[_, _]]) {
      return (obj: AnyRef) ⇒ obj.asInstanceOf[scala.collection.Map[String, _]].getOrElse(nodeName, null)
    } else if (isNumber(nodeName)) {
      val idx = Integer.parseInt(nodeName)
      if (parmObj.getClass.isArray) {
        return (obj: AnyRef) ⇒ java.lang.reflect.Array.get(obj, idx)
      } else if (parmObj.isInstanceOf[java.util.List[_]]) {
        return (obj: AnyRef) ⇒ obj.asInstanceOf[java.util.List[Any]].get(idx)
      } else if (parmObj.isInstanceOf[scala.collection.Seq[_]]) {
        return (obj: AnyRef) ⇒ obj.asInstanceOf[scala.collection.Seq[Any]].apply(idx)
      } else {
        throw new IllegalArgumentException("Cannot access index %d on %s".format(idx, parmObj.getClass))
      }
    } else {
      var method: java.lang.reflect.Method = null
      java.beans.Introspector.getBeanInfo(parmObj.getClass).getPropertyDescriptors.foreach { pd ⇒
        if (method == null && nodeName == pd.getName) {
          method = pd.getReadMethod
        }
      }
      if (method == null) try {
        method = parmObj.getClass.getMethod(nodeName)
      } catch {
        case _: NoSuchMethodException ⇒ try {
          val field = parmObj.getClass.getField(nodeName)
          return (obj: AnyRef) ⇒ field.get(obj)
        } catch {
          case _: NoSuchFieldException ⇒ throw new IllegalArgumentException("Cannot find value %s on %s".format(nodeName, parmObj.getClass))
        }
      }
      assert(method != null)
      return (obj: AnyRef) ⇒ method.invoke(obj)
    }

  }

  private def unOption(obj: Any): AnyRef = obj match {
    case Some(o: AnyRef) ⇒ o
    case None ⇒ null
    case null ⇒ null
    case o: AnyRef ⇒ o
  }

  private val splitter = new scala.util.matching.Regex("""[\.\[\]]""")

  def leafValue(rootObj: AnyRef, nodePath: String): Any = {
    var node = unOption(rootObj)
    val nodeNames = splitter.split(nodePath)
    var i = 0
    while (i < nodeNames.length && node != null) {
      node = unOption(nodeValue(node, nodeNames(i)))
      i += 1
    }
    node
  }

  private def findStaticInstance[T](cls: Class[T]): Option[T] = {
    import java.lang.reflect.Modifier
    var instance: Option[T] = None
    for (field ← cls.getFields)
      if (field.getType == cls && Modifier.isStatic(field.getModifiers))
        instance = Some(field.get(null).asInstanceOf[T])
    instance
  }

  def getInstance[T](cls: Class[T]): T = {
    findStaticInstance(cls) match {
      case Some(i) ⇒ return i
      case None ⇒ // Continue below
    }
    val clsName = cls.getName
    if (!clsName.endsWith("$"))
      findStaticInstance(Class.forName(clsName concat "$")) match {
        case Some(i) ⇒ return i.asInstanceOf[T]
        case None ⇒
      }
    throw new org.orbroker.exception.ConfigurationException("Cannot get instance of " + clsName)
  }
}
