# contrib/pg_retire/Makefile

MODULE_big = pg_retire
OBJS = pg_retire.o $(WIN32RES)
PGFILEDESC = "pg_retire - terminate normal backend after after client down"


ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_retire
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
