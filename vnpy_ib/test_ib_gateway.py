from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock

from ibapi.contract import Contract
from ibapi.execution import Execution

from vnpy_ib.ib_gateway import IbApi


class TestIbGateway(TestCase):

    def test_SHOULD_convert_EST_date_properly(self):
        # given
        contract = a_contract()
        execution = an_execution(time='20230424 17:15:00 EST')

        ib_gateway = Mock()
        ib_api = IbApi(gateway=ib_gateway)

        # when
        ib_api.execDetails(reqId=1, contract=contract, execution=execution)

        # then
        ib_gateway.on_trade.assert_called_once()
        got_date = ib_gateway.on_trade.call_args[0][0].datetime.strftime('%Y%m%d %H:%M:%S %Z')

        self.assertEqual('20230425 00:15:00 CEST', got_date)


def a_contract():
    return Contract()


def a_time_str():
    datetime.now().strftime('%Y%m%d %H:%M:%S %Z')


def an_execution(time=a_time_str(), side='BOT'):
    execution = Execution()
    execution.time = time
    execution.side = side
    return execution
