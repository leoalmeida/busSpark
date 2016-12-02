#!/usr/bin/env python

from operator import itemgetter

import os
import sys

last_line = None
line_count = 0

filename = '/readings-' + os.getenv('mapred_task_id')
proc = subprocess.Popen(['hadoop', 'fs', '-put', '-', filename], stdin=subprocess.PIPE)


for line in sys.stdin:

    # line = line.strip()
    # Mov_Idlinha, Mov_longitude, Mov_latitude, Mov_Idponto = line.split("\t")
    # Mov_Idlinha, Mov_line = line.split("\t")
    (key, value) = line.rstrip().split('\t')

    # if this is the first iteration
    # if not last_line:
    #     last_line = Mov_Idlinha
    #     type = "{\"type\": \"table\""
    #     name = "\"name\": \"readings_{0}\"".format(Mov_Idlinha)
    #     columns = "\"columns\": [{\"name\": \"dtservidor\", \"type\": \"TEXT\"},\n"
    #     columns = "%s {\"name\": \"dtavl\", \"type\": \"TEXT\"},\n" % (columns)
    #     columns = "%s {\"name\": \"idlinha\", \"type\": \"INT\"},\n" % (columns)
    #     columns = "%s {\"name\": \"lat\", \"type\": \"REAL\"},\n" % (columns)
    #     columns = "%s {\"name\": \"lon\", \"type\": \"REAL\"},\n" % (columns)
    #     columns = "%s {\"name\": \"idavl\", \"type\": \"TEXT\"},\n" % (columns)
    #     columns = "%s {\"name\": \"evento\", \"type\": \"INT\"},\n" % (columns)
    #     columns = "%s {\"name\": \"idponto\", \"type\": \"NUM\"}]" % (columns)
    #     rows = "\"rows\":["
    #     print(",\n".join(str(v) for v in [type, name, columns, rows]))

    # if they're the same, log it
    # if Mov_Idlinha == last_line:
    # line_count += 1
    # else:
    # result = [last_line, line_count]
    # print("\t".join(str(v) for v in result))
    # last_line = Mov_Idlinha
    # line_count = 1

    # print("[" + ",".join(str(v) for v in [Mov_Idlinha, Mov_longitude, Mov_latitude, Mov_Idponto])) + "],"

    # in normal case, write to output file
    if key != prevKey:
        print "[" + value + "],"
    # in other case, use "hadoop fs -put"
    else:
        proc.stdin.write("[" + value + "],\n")

    # line = "[" + Mov_line + "],"
    # results = [Mov_Idlinha, line]
    # print("\t".join(results))

# close pipe
proc.stdin.close()
# wait for subprocess
proc.wait()