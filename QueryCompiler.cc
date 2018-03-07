#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include <limits>
#include <ctime>

#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"

using namespace std;


QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) :
	catalog(&_catalog), optimizer(&_optimizer) {
}

QueryCompiler::~QueryCompiler() {
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate, NameList* _groupingAtts,
	int& _distinctAtts,	QueryExecutionTree& _queryTree) {
	// store Scans and Selects for each table to generate Query Execution Tree
	unordered_map<string, RelationalOp*> pushDowns;
	TableList *tblList = _tables;
	while(_tables != NULL) {
		string tableName = string(_tables->tableName);
		DBFile dbFile; string dbFilePath;		
		if(!catalog->GetDataFile(tableName, dbFilePath)) {
			cerr << "ERROR: Table '" << tableName << "' does not exist." << endl << endl;
			exit(-1);
		}
		char* dbFilePathC = new char[dbFilePath.length()+1]; 
		strcpy(dbFilePathC, dbFilePath.c_str());
		if(dbFile.Open(dbFilePathC) == -1) {
			// error message is already shown in File::Open
			exit(-1);
		}

		/** create a SCAN operator for each table in the query **/
		Schema schema;
		if(!catalog->GetSchema(tableName, schema)) {
			cerr << "ERROR: Table '" << tableName << "' does not exist." << endl << endl;
			exit(-1);
		}

		/** push-down selects: create a SELECT operator wherever necessary **/

		// put Scan in pushDowns first, and will be replaced if predicate exists
		Scan* scan = new Scan(schema, dbFile);
		pushDowns[tableName] = (RelationalOp*) scan;

		// get the predicate for this table
		if(_predicate != NULL) {
			CNF cnf; Record literal;
			// CNF::ExtractCNF returns 0 if success, -1 otherwise
			if(cnf.ExtractCNF(*_predicate, schema, literal) == -1) {
				// error message is already shown in CNF::ExtractCNF
				exit(-1);
			}

			// schema only for Select
			Schema schSelect = schema;
			vector<int> attsToPushDown;

			if(cnf.numAnds > 0) {
				// now, look up indices
				bool hasNothing = false;
				string indexFilePath, attrName;
				int whichAtt, pointer, target; bool isNameLeft; CompOperator oper;
				vector<DBFile*> indexFiles; 
				vector<int> attsToIndex; vector<pair<int, int>> ranges;
				unordered_set<int> visited; // prevent duplicate in case of range query
				for(int i = 0; i < cnf.numAnds; i++) {
					if(cnf.andList[i].operand1 != Literal) {
						whichAtt = cnf.andList[i].whichAtt1; isNameLeft = true;
						pointer = ((int *) literal.GetBits())[cnf.andList[i].whichAtt2 + 1];
						target = *(int *)&(literal.GetBits()[pointer]);
					} else if(cnf.andList[i].operand2 != Literal) {
						whichAtt = cnf.andList[i].whichAtt2; isNameLeft = false;
						pointer = ((int *) literal.GetBits())[cnf.andList[i].whichAtt1 + 1];
						target = *(int *)&(literal.GetBits()[pointer]);
					} else { break; }
					oper = cnf.andList[i].op; // LessThan | GreaterThan | Equals

					if(visited.find(whichAtt) == visited.end()) {
						attrName = schema.GetAtts()[whichAtt].name;
						if(catalog->GetIndex(tableName, attrName, indexFilePath)) {
							// if one matches, create IndexScan and put it into pushDowns
							DBFile* indexFile = new DBFile();
							char* indexFilePathC = new char[indexFilePath.length()+1];
							strcpy(indexFilePathC, indexFilePath.c_str());
							if(indexFile->Open(indexFilePathC) == -1) {
								// error message is already shown in File::Open
								exit(-1);
							}
							indexFile->InitBPlusTreeNodeSchema();

							indexFiles.push_back(indexFile);
							attsToIndex.push_back(whichAtt);
							// create range for this predicate
							if((isNameLeft && oper == GreaterThan) || 
								(!isNameLeft && oper == LessThan)) { // lower
								ranges.push_back(make_pair(target, numeric_limits<int>::max()));
							} else if((isNameLeft && oper == LessThan) || 
								(!isNameLeft && oper == GreaterThan)) { // upper
								ranges.push_back(make_pair(numeric_limits<int>::min(), target));
							} else { // equal (lower = upper)
								ranges.push_back(make_pair(target, target));
							}
						} else { // predicate does not match index, so create Select
							attsToPushDown.push_back(whichAtt);
						}
						visited.insert(whichAtt);
					} else if(find(attsToIndex.begin(), attsToIndex.end(), whichAtt) != attsToIndex.end()) {
						// at this point, range should be updated
						for(size_t j = 0; j < attsToIndex.size(); j++) {
							if(whichAtt == attsToIndex[j]) {
								if((isNameLeft && oper == GreaterThan) || 
									(!isNameLeft && oper == LessThan)) { // lower
									if(target > ranges[j].first)
										ranges[j].first = target;
								} else if((isNameLeft && oper == LessThan) || 
									(!isNameLeft && oper == GreaterThan)) { // upper
									if(target < ranges[j].second)
										ranges[j].second = target;
								} else { // equal (lower = upper)
									if(target > ranges[j].first && target < ranges[j].second) {
										// if the point target inside the range, update
										ranges[j].first = target;
										ranges[j].second = target;
									} else if(target == ranges[j].first && target == ranges[j].second) {
										// skip if we have duplicate predicate
										continue;
									} else { // this will always return 0 record
										hasNothing = true;
									}
								}
								break;
							}
						}
					}
				}

				// otherwise, create Scan from Select
				if(attsToIndex.size() > 0) {
					// first, create IndexScan
					// if possible, let's create a function to project CNF
					// so that IndexScan only has indexed predicates
					IndexScan* indexScan = new IndexScan(schema, cnf, literal, dbFile, 
						indexFiles, attsToIndex, ranges, hasNothing);
					
					if(attsToPushDown.size() > 0) { // create Select if necessary
						Select* select = new Select(schema, cnf, literal, (RelationalOp*) indexScan);
						pushDowns[tableName] = (RelationalOp*) select;
					} else { // if no Select, just put IndexScan into pushDowns
						pushDowns[tableName] = (RelationalOp*)indexScan;
					}					
				} else {
					Select* select = new Select(schema, cnf, literal, (RelationalOp*) scan);
					pushDowns[tableName] = (RelationalOp*) select;
				}
			}
		}

		// move on to the next table
		_tables = _tables->next;
	}

	/** call the optimizer to compute the join order **/
	OptimizationTree root;
	optimizer->Optimize(tblList, _predicate, &root);
	OptimizationTree *rootTree = &root;
	/** create join operators based on the optimal order computed by the optimizer **/
	// get actual Query eXecution Tree by joining them
	RelationalOp* qxTree = buildJoinTree(rootTree, _predicate, pushDowns, 0);

	/** create the remaining operators based on the query **/
	// qxTreeRoot will be the root of query execution tree
	RelationalOp* qxTreeRoot = (RelationalOp*) qxTree;

	// case 1. SELECT-WHERE-FROM
	// since there is no _groupingAtts, check _finalFunction first.
	// if _finalFunction does not exist, Project and check _distinctAtts for DuplicateRemoval
	// else, append Sum at the root.

	// schemaIn always comes from join tree
	Schema schemaIn = qxTree->GetSchema();

	if(_groupingAtts == NULL) {

		if(_finalFunction == NULL) { // check _finalFunction first
			// a Project operator is appended at the root
			Schema schemaOut = schemaIn;
			int numAttsInput = schemaIn.GetNumAtts();
			int numAttsOutput = 0;
			vector<int> attsToKeep;
			vector<Attribute> atts = schemaIn.GetAtts();
			bool isFound;

			while(_attsToSelect != NULL) {
				string attrName = string(_attsToSelect->name);
				isFound = false;
				for(int i = 0; i < atts.size(); i++) {
					if(atts[i].name == attrName) {
						isFound = true;
						attsToKeep.push_back(i);
						numAttsOutput++;
						break;
					}
				}

				if(!isFound) {
					cerr << "ERROR: Attribute '" << attrName << "' does not exist." << endl << endl;
					exit(-1);
				}

				_attsToSelect = _attsToSelect->next;
			}

			// reverse the vector; _attsToSelect has reverse order of attributes
			reverse(attsToKeep.begin(), attsToKeep.end());

			if(schemaOut.Project(attsToKeep) == -1) {
				cerr << "ERROR: Project failed:\n" << schemaOut << endl << endl;
				exit(-1);
			}

			int* keepMe = new int[attsToKeep.size()];
			copy(attsToKeep.begin(), attsToKeep.end(), keepMe);

			Project* project = new Project(schemaIn, schemaOut, numAttsInput, numAttsOutput,
				keepMe, qxTree);

			// in case of DISTINCT, a DuplicateRemoval operator is further inserted
			if(_distinctAtts != 0) {
				Schema schemaIn = project->GetSchema();
				DuplicateRemoval* distinct = new DuplicateRemoval(schemaIn, project);
				qxTreeRoot = (RelationalOp*) distinct;
			} else {
				qxTreeRoot = (RelationalOp*) project;
			}
		} else { // a Sum operator is insert at the root
			// the output schema consists of a single attribute 'sum'.
			vector<string> attributes, attributeTypes;
			vector<unsigned int> distincts;

			Function compute; compute.GrowFromParseTree(_finalFunction, schemaIn);

			attributes.push_back("sum");
			attributeTypes.push_back(compute.GetTypeAsString());
			distincts.push_back(1);
			Schema schemaOut(attributes, attributeTypes, distincts);

			Sum* sum = new Sum(schemaIn, schemaOut, compute, qxTree);
			qxTreeRoot = (RelationalOp*) sum;
		}
	} else { // case 2. SELECT-FROM-WHERE-GROUPBY
		// if query has GROUP BY, a GroupBy operator is appended at the root

		// the output schema contains the aggregate attribute 'sum' on the first position
		vector<string> attributes, attributeTypes;
		vector<unsigned int> distincts;

		// followed by the grouping attributes
		vector<int> attsToGroup; int attsNo = 0;

		while(_groupingAtts != NULL) {
			string attrName = string(_groupingAtts->name);
			int noDistinct = schemaIn.GetDistincts(attrName);
			if(noDistinct == -1) {
				cerr << "ERROR: Attribute '" << attrName << "' does not exist." << endl << endl;
				exit(-1);
			}
			Type attrType = schemaIn.FindType(attrName); string attrTypeStr;
			switch(attrType) {
				case Integer:
					attrTypeStr = "INTEGER";
					break;
				case Float:
					attrTypeStr = "FLOAT";
					break;
				case String:
					attrTypeStr = "STRING";
					break;
				default:
					attrTypeStr = "UNKNOWN";
					break;
			}

			attributes.push_back(attrName);
			attributeTypes.push_back(attrTypeStr);
			distincts.push_back(noDistinct);

			attsToGroup.push_back(schemaIn.Index(attrName));
			attsNo++;

			_groupingAtts = _groupingAtts->next;
		}

		// put aggregate function at the end due to reverse afterward
		Function compute;
		if(_finalFunction != NULL) {
			compute.GrowFromParseTree(_finalFunction, schemaIn);

			attributes.push_back("sum");
			attributeTypes.push_back(compute.GetTypeAsString());
			distincts.push_back(1);
		}

		// reverse the vectors; _groupingAtts has reverse order of attributes
		reverse(attributes.begin(), attributes.end());
		reverse(attributeTypes.begin(), attributeTypes.end());
		reverse(distincts.begin(), distincts.end());
		reverse(attsToGroup.begin(), attsToGroup.end());

		Schema schemaOut(attributes, attributeTypes, distincts);

		int* attsOrder = new int[attsToGroup.size()];
		copy(attsToGroup.begin(), attsToGroup.end(), attsOrder);
		OrderMaker groupingAtts(schemaIn, attsOrder, attsNo);

		GroupBy* group = new GroupBy(schemaIn, schemaOut, groupingAtts, compute, qxTree);
		qxTreeRoot = (RelationalOp*) group;
	}

	// in the end, create WriteOut at the root of qxTree
	
	// for in cases of comparing results,
	// get current datetime to store result in the separate file
	// comment these lines out when submit
	time_t timeT; struct tm* timeInfo; char buf[80]; string timeS;
	time(&timeT); timeInfo = localtime(&timeT);
	strftime(buf, sizeof(buf),"%Y%m%d_%H%M%S", timeInfo);
	timeS = string(buf);
	string outFile = "output/" + timeS + ".txt";
	// string outFile = "output.txt";
	Schema outSchema = qxTreeRoot->GetSchema();
	WriteOut* writeOut = new WriteOut(outSchema, outFile, qxTreeRoot);
	qxTreeRoot = (RelationalOp*) writeOut;

	/** connect everything in the query execution tree and return **/
	_queryTree.SetRoot(*qxTreeRoot);

	/** free the memory occupied by the parse tree since it is not necessary anymore **/
	_tables = NULL; _attsToSelect = NULL; _finalFunction = NULL;
	_predicate = NULL; _groupingAtts = NULL; rootTree = NULL;
}

// a recursive function to create Join operators (w/ Select & Scan) from optimization result
RelationalOp* QueryCompiler::buildJoinTree(OptimizationTree*& _tree,
	AndList* _predicate, unordered_map<string, RelationalOp*>& _pushDowns, int depth) {
	// at leaf, do push-down (or just return table itself)
	if(_tree->leftChild == NULL && _tree->rightChild == NULL) {
		return _pushDowns.find(_tree->tables[0])->second;
	} else { // recursively do join from left/right RelOps
		Schema lSchema, rSchema, oSchema; CNF cnf;

		RelationalOp* lOp = buildJoinTree(_tree->leftChild, _predicate, _pushDowns, depth+1);
		RelationalOp* rOp = buildJoinTree(_tree->rightChild, _predicate, _pushDowns, depth+1);

		lSchema = lOp->GetSchema();
		rSchema = rOp->GetSchema();
		cnf.ExtractCNF(*_predicate, lSchema, rSchema);
		oSchema.Append(lSchema); oSchema.Append(rSchema);
		Join* join = new Join(lSchema, rSchema, oSchema, cnf, lOp, rOp);

		// set current depth for join operation
		join->depth = depth;
		join->numTuples = _tree->noTuples;

		return (RelationalOp*) join;
	}
}
