V = 0.1
CONFIG = ./config

include $(CONFIG)

OBJS = src/luasql.o src/lua_oci.o
SRCS = src/luasql.h src/luasql.c src/lua_oci.c

all: $(TARGET)

$(TARGET) : $(OBJS) 
	$(CC) -o $(TARGET) $(LIB_OPTION) $(OBJS) $(DRIVER_LIBS) $(INT64_LDFLAGS)

src/.c.o: $(SRCS)
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -f *.so src/*.o
