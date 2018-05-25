import sys, traceback, time

from _informixcdc import *

from _informixcdc import InformixCdc as _InformixCdc

class InformixCdc(_InformixCdc):
    __doc__ = _InformixCdc.__doc__

def main():
    import pprint
    cdc = InformixCdc('informix_1', timeout=1, max_records=20,
                      lo_buffer_sz=1000, use_savepoints=False)

    print 'connect()', cdc.connect('informix', 'informix')
    print 'is_connected', cdc.is_connected
    print 'session_id', cdc.session_id
    print 'enable()', cdc.enable(database="informixcdc_test",
                                 owner="informix",
                                 table="informixcdc_test",
                                 columns = "cdc_serial8, cdc_bigint_low, cdc_bigint_high,"
                                 "cdc_char, cdc_date, cdc_datetime, cdc_decimal_low,"
                                 "cdc_decimal_high, cdc_float_low, cdc_float_high,"
                                 "cdc_integer_low, cdc_integer_high, cdc_smallfloat_low,"
                                 "cdc_smallfloat_high, cdc_smallint_low, cdc_smallint_high,"
                                 "cdc_varchar, cdc_lvarchar")
    print 'activate()', cdc.activate(seq_number=287784092040)

    pp = pprint.PrettyPrinter(indent=4)

    counter = 0
    try:
        for record in cdc:
            counter += 1
            if counter % 10000 == 0:
                print counter
                pp.pprint(record)

        if counter % 10000 != 0:
            print counter
    except:
        print '-'*60
        traceback.print_exc(file=sys.stdout)
        print '-'*60

if __name__ == '__main__':
    main()
