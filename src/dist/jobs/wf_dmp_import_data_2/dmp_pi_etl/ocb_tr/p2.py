#!/app/python/bin/python
import sys

for line in sys.stdin:

    op = line.rstrip('\n')
    ph = op.split('/')[1]
    print 'hadoop fs -mv', line.rstrip('\n'), 'dmp_pi_store_db/' + ph + '/data_source_id=5'

