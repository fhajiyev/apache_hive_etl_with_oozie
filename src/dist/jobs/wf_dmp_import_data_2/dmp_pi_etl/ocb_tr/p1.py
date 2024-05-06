#!/app/python/bin/python
import sys

for line in sys.stdin:

    op = line.rstrip('\n')
    ph = op.split('/')[1]
    print 'hadoop fs -mv', line.rstrip('\n'), 'backup_d5/' + ph

