MODULE_FLAGS := -fPIC -O2 -Wall -Werror

OBJECT_FILES := modmain.o
MODSNAP_CXX_FLAGS := -std=gnu++14

%.o: %.cpp
	$(CXX) -o $@ -c $< $(MODULE_FLAGS) -I../../../deps/cpp-statsd-client/include $(MODSNAP_CXX_FLAGS) -g

modstatsd.so: $(OBJECT_FILES)
	$(CXX) -shared $(OBJECT_FILES) -o modstatsd.so

clean:
	rm -f $(OBJECT_FILES) modstatsd.so
