#include <iostream>
#include <cstring>
#include <unistd.h>

#include "Catalog.h"
#include "QueryOptimizer.h"
#include "QueryCompiler.h"
#include "RelOp.h"
#include "TableSetter.h"
extern "C" { // due to "previous declaration with ‘C++’ linkage"
	#include "QueryParser.h"
}

using namespace std;


// these data structures hold the result of the parsing
extern struct FuncOperator* finalFunction; // the aggregate function
extern struct TableList* tables; // the list of tables in the query
extern struct AndList* predicate; // the predicate in WHERE
extern struct NameList* groupingAtts; // grouping attributes
extern struct NameList* attsToSelect; // the attributes in SELECT
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern char* command; // command to execute other functionalities (e.g. exit)
extern char* table; // single table name for CREATE and LOAD
extern struct AttrAndTypeList* attrAndTypes; // attributes and types to be inserted
extern char* textFile; // text file to be loaded into a table
extern char* indexName; // index name for CREATE INDEX
extern char* attr; // single attribute name for CREATE INDEX

extern "C" int yyparse();
extern "C" int yylex_destroy();

int NUM_PAGES_AVAILABLE = 100; // default number of pages available in a DBFile

int main (int argc, char* argv[]) {
	// this is the catalog
	string catalogFileName = "catalog.sqlite";
	Catalog catalog(catalogFileName);

	// this is the query optimizer
	// it is not invoked directly but rather passed to the query compiler
	QueryOptimizer optimizer(catalog);

	// this is the query compiler
	// it includes the catalog and the query optimizer
	QueryCompiler compiler(catalog, optimizer);

	TableSetter tableSetter(catalog);

	// set NUM_PAGES_AVAILABLE from the first argument
	if(argc == 2) {
		NUM_PAGES_AVAILABLE = atoi(argv[1]);
	}

	while(true) {
		cout << "sqlite-jarvis> ";

		// the query parser is accessed directly through yyparse
		// this populates the extern data structures
		int parse = -1;
		if (yyparse() == 0) {
			parse = 0;
		} else {
			cout << "Error: Query is not correct!" << endl << endl;
			parse = -1;
		}

		yylex_destroy();

		if(parse == 0) {			
			if(table != NULL && attrAndTypes != NULL) { // CREATE TABLE
				tableSetter.createTable(table, attrAndTypes);
			} else if(table != NULL && textFile != NULL) { // LOAD DATA
				tableSetter.loadData(table, textFile);
			} else if(indexName != NULL && table != NULL && attr != NULL) { // CREATE INDEX
				tableSetter.createIndex(indexName, table, attr);
			} else if(indexName != NULL) { 
				tableSetter.dropIndex(indexName);
			} else if(table != NULL) { // DROP TABLE
				tableSetter.dropTable(table);
			} else if(command != NULL) { // single word commands
				if(strcmp(command, "exit") == 0 || strcmp(command, "quit") == 0) {
					// exit, quit
					cout << "Bye!" << endl << endl;
					return 0;
				} else if(strcmp(command, "schema") == 0) {
					cout << catalog << endl << endl;
				} else if(strcmp(command, "index") == 0) {
					cout << catalog.PrintIndex() << endl << endl;
				} else if(strcmp(command, "save") == 0) {
					if(catalog.Save())
						cout << "OK!" << endl << endl;
				} else {
					cout << endl << "Error: Command not found." << endl << endl;
				}
			} else {
				cout << endl << "OK!" << endl;

				// at this point we have the parse tree in the ParseTree data structures
				// we are ready to invoke the query compiler with the given query
				// the result is the execution tree built from the parse tree and optimized
				QueryExecutionTree queryTree;
				compiler.Compile(tables, attsToSelect, finalFunction, predicate,
					groupingAtts, distinctAtts, queryTree);

				cout << queryTree << endl;

				queryTree.ExecuteQuery();
			}
		}

		// detect input redirection
		if(!isatty(STDIN_FILENO)) { return 0; }

		// reset all vars
		finalFunction = NULL; tables = NULL; predicate = NULL; 
		groupingAtts = NULL; attsToSelect = NULL; distinctAtts = 0;
		command = NULL; table = NULL; attrAndTypes = NULL;
		textFile = NULL; indexName = NULL; attr = NULL;
	}

	return 0;
}
