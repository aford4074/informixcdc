from _informixcdc import *

from _informixcdc import InformixCdc as _InformixCdc

class InformixCdc(_InformixCdc):
    __doc__ = _InformixCdc.__doc__

def tests():
    from time import sleep

    cdc = InformixCdc('informix_1', timeout=10, max_records=100)
    print 'connect()', cdc.connect('informix', 'informix')
    print 'is_connected', cdc.is_connected
    print 'session_id', cdc.session_id
    print 'enable()', cdc.enable("stocks:informix.cdc_test", "field1,field2")
    print 'activate()', cdc.activate()

    from collections import defaultdict
    from gc import get_objects
    before = defaultdict(int)
    for i in get_objects():
        before[type(i)] += 1

    x = 0
    for record in cdc:
        print record.__repr__()
        x += 1
        if x % 250 == 0:
            after = defaultdict(int)
            for i in get_objects():
                after[type(i)] += 1
            print [(k, after[k] - before[k]) for k in after if after[k] - before[k]]

if __name__ == '__main__':
    tests()
