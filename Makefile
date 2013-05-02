cql: cql.rs lib
	rustc -O -L./ $< -o cql

lib: src/cql_client.rs
	rustc --lib -O $< --out-dir=./

all: cql

clean:
	rm libcql_client* cql
