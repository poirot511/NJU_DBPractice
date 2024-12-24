# 编译项目
copy:
	cd build; \
	cmake .. -DCMAKE_BUILD_TYPE=Debug; \
	make

t1_0:
	cd test/sql/lab02 && bash evaluate.sh ../../../build/bin t1

# 测试目标 t1_1
t1_1:
	cd test/sql/lab02 && bash evaluate.sh ../../../build/bin t1/01_prepare_table_dbcourse.sql

# 测试目标 t1_2
t1_2:
	cd test/sql/lab02 && bash evaluate.sh ../../../build/bin t1/02_seqscan_limit_projection.sql

# 测试目标 t1_3
t1_3:
	cd test/sql/lab02 && bash evaluate.sh ../../../build/bin t1/03_filter_update_delete.sql

# 测试目标 t1_4
t1_4:
	cd test/sql/lab02 && bash evaluate.sh ../../../build/bin t1/04_sort_final.sql

# 测试目标 t1_5
t1_5:
	cd test/sql/lab02 && bash evaluate.sh ../../../build/bin t1/05_cleanup.sql

# 清理构建文件
clean:
	rm -rf build/*