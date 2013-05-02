lib: src/cql_client.rs
	rustc --lib -O $< --out-dir=./

cql: cql.rs
	rustc -O -L./ $< -o cql

all: cql

clean:
	rm libcql_client* cql
