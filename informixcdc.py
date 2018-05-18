from _informixcdc import *

from _informixcdc import InformixCdc as _InformixCdc

class InformixCdc(_InformixCdc):
    __doc__ = _InformixCdc.__doc__

def tests():
    cdc = InformixCdc('informix_1', timeout=10, max_records=1)
    print 'connect()', cdc.connect('informix', 'informix')
    print 'is_connected', cdc.is_connected
    print 'session_id', cdc.session_id
    print 'enable()', cdc.enable("stocks:informix.cdc_test", "field1,field2")
    print 'activate()', cdc.activate()

    while True:
        print cdc.read().__repr__()
        print 'read stopped blocking'

if __name__ == '__main__':
    tests()
