copy:
	cd build; \
	cmake .. -DCMAKE_BUILD_TYPE=Debug

t1_1: copy
	cd build; \
	make replacer_test; \
	./bin/replacer_test

t1_2: copy
	cd build; \
	make buffer_pool_test; \
	./bin/buffer_pool_test

t1_3: copy
	cd build; \
	make table_handle_test; \
	./bin/table_handle_test

all: t1_1 t1_2 t1_3

clean:
	rm -rf build/*