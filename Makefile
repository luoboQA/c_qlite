c_qlite: c_qlite.c
	gcc c_qlite.c -o c_qlite

run: c_qlite
	./c_qlite mydb.db

clean:
	rm -f c_qlite *.c_qlite

test: c_qlite
	bundle exec rspec


