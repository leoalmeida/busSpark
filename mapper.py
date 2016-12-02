#!/usr/bin/env python

# COLUNA	CAMPO	        DESCRICAO
# COLUNA 0	Mov_dtservidor	DATA SERVIDOR APN
# COLUNA 1	Mov_dtavl	    DATA DO AVL
# COLUNA 2	Mov_Idlinha	    CODIGO DA LINHA (TABELA AVA)
# COLUNA 3	Mov_longitude	LONGITUDE
# COLUNA 4	Mov_latitude	LATITUDE
# COLUNA 5	Mov_Idavl	    PREFIXO (TABELA AV)
# COLUNA 6	Mov_Evento	    EVENTO VIAGEM (0=ABERTA 64=FECHADA)
# COLUNA 7	Mov_Idponto	    PONTO NOTAVEL

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    unpacked = line.split(",")

    if len(unpacked) != 8:
        # Something has gone wrong. Skip this line.
        continue

    Mov_dtservidor, Mov_dtavl, Mov_Idlinha, Mov_longitude, Mov_latitude, Mov_Idavl, Mov_Evento, Mov_Idponto = unpacked
    # lineprint = "{0}, \"{1}\", \"{2}\", {3}, {4}, {5}, {6}, {7}".format(Mov_Idlinha, Mov_dtservidor, Mov_dtavl, Mov_longitude, Mov_latitude, Mov_Idponto, Mov_Idavl, Mov_Evento)
    results = [Mov_Idlinha, line]
    print("\t".join(results))
