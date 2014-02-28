package org.orbroker.adapt

/**
 * Broker adapter. Unifies all the adapters.
 * @author Nils Kilden-Pedersen
 */
trait BrokerAdapter
  extends ExceptionAdapter
  with ParameterAdapter
  with ColumnNameAdapter
  with StatementAdapter

/**
 * The default adapter.
 * @author Nils Kilden-Pedersen
 */
private[orbroker] object DefaultAdapter extends DefaultAdapter

/**
 * The default adapter, can be used for maintaining default behavior, but
 * overriding one or more traits.
 * @author Nils Kilden-Pedersen
 */
class DefaultAdapter
  extends BrokerAdapter
  with DefaultExceptionAdapter
  with DefaultParameterAdapter
  with DefaultColumnNameAdapter
  with DefaultStatementAdapter