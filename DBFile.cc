#include <sys/stat.h>
#include <cstring>
#include <vector>
#include <sstream>

#include "DBFile.h"

using namespace std;


DBFile::DBFile () : 
	fileName(""), 
	isMovedFirst(false), 
	isTreeTraversed(false) {
}

DBFile::~DBFile () {
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	
	fileName(_copyMe.fileName), 
	isMovedFirst(false), 
	isTreeTraversed(false) {
}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;

	return *this;
}

int DBFile::Create (char* f_path, FileType f_type) {
	fileName = f_path;
	fileType = f_type;

	// return 0 on success, -1 otherwise
	return file.Open(0, f_path); // mode = O_TRUNC | O_RDWR | O_CREAT;
}

int DBFile::Open (char* f_path) {
	fileName = f_path;

	// check whether file exists or not
	struct stat fileStat;
	if(stat(f_path, &fileStat) != 0) {
		return Create(f_path, Heap);
	} else {
		// return 0 on success, -1 otherwise
		return file.Open(fileStat.st_size, f_path); // mode = O_RDWR;
	}
}

void DBFile::Load (Schema& schema, char* textFile) {
	MoveFirst();
	FILE* textData = fopen(textFile, "r");

	while(true) {
		Record record;
		if(record.ExtractNextRecord(schema, *textData)) { // success on extract
			AppendRecord(record);
		} else { // no data left or error
			break;
		}
	}
	WriteToFile(); // add the last page to the file
	fclose(textData);
}

int DBFile::Close () {
	int ret = file.Close();
	if(ret == -1)
		cerr << "ERROR: Failed to close DBFile." << endl << endl;

	return ret;
}

void DBFile::MoveFirst () {
	iPage = 0; // reset page index to 0
	isMovedFirst = true;
	pageNow.EmptyItOut(); // the first page has no data
}

void DBFile::AppendRecord (Record& rec) {
	if(!pageNow.Append(rec)) { // no space in the current page, pageNow
		WriteToFile(); // add pageNow to the file
		pageNow.Append(rec); // add rec to the pageNow
	}
}

void DBFile::WriteToFile() {
	file.AddPage(pageNow, iPage++);
	pageNow.EmptyItOut(); // clear pageNow
}

int DBFile::GetNext (Record& rec) {
	if(!isMovedFirst) {
		MoveFirst();
	}

	off_t numPage = file.GetLength();
	while(true) {
		if(!pageNow.GetFirst(rec)) { // no record in the current page
			isNewPage = true;
			// check whether this is the last page
			if(iPage == numPage) { // EOF
				break;
			} else { // move on to the next page
				file.GetPage(pageNow, iPage++);
				treeNodePtr = iPage;
			}
		} else { // record exists
			return 0;
		}
	}
	return -1;
}

int DBFile::GetRecord(Record& putItHere, off_t whichPage, off_t whichRecord) {
	return file.GetRecord(putItHere, whichPage, whichRecord);
}

string DBFile::GetTableName() {
	vector<string> path;
	stringstream ss(fileName); string tok;
	while(getline(ss, tok, '/')) {
		path.push_back(tok);
	}
	string tempFileName = path[path.size()-1];
	stringstream ss2(tempFileName); string tableName = "";
	getline(ss2, tableName, '.');
	return tableName;
}

const char* DBFile::GetFileName() {
	char* fileNameC = new char[fileName.length()+1];
	strcpy(fileNameC, fileName.c_str());
	return fileNameC;
}

int DBFile::GetCurrentPageNum() {
	return iPage;
}

void DBFile::LoadBPlusTree(BPlusTree& _tree) {
	// read b+ tree and write to the DBFile
	InitBPlusTreeNodeSchema();
	MoveFirst();

	Node* node; int nodeNum = 1; // root starts from page #1
	Record rec; // record to be written in DBFile
	int* attrs = new int[3]; // container for attributes
	while(_tree.GetNext(node)) { // BFS
		int numKeys = node->keys.size();
		if(!node->isLeaf) { // InternalNode
			InternalNode* iNode = (InternalNode*)node;

			// first, create header record
			attrs[0] = 0; // is_leaf = false
			attrs[1] = ++nodeNum; // left_most_ptr
			attrs[2] = numKeys; // num_recs
			rec.extractBPlusTreeNode(attrs);
			// rec.show(schInterHeader);
			AppendRecord(rec);

			// and then other records for body
			for(int i = 0; i < numKeys; i++) {
				attrs[0] = iNode->keys[i]; // key
				attrs[1] = ++nodeNum; // ptr
				attrs[2] = iNode->isDuplicate[i] ? 1 : 0; // is_duplicate
				rec.extractBPlusTreeNode(attrs);
				// if(i == 0 || i == numKeys-1) {
				// 	rec.show(schInter);
				// }
				AppendRecord(rec);
			}

			// in the end, finalize the page (=a node)
			WriteToFile(); // add pageNow to the file
			iNode = NULL;

		} else { // LeafNode
			LeafNode* leaf = (LeafNode*)node;

			// first, create header record
			attrs[0] = 1; // is_leaf = true
			attrs[1] = iPage+2; // right_most_ptr
			attrs[2] = numKeys; // num_recs
			rec.extractBPlusTreeNode(attrs);
			// rec.show(schLeafHeader);
			AppendRecord(rec);

			// and then other records for body
			for(int i = 0; i < numKeys; i++) {
				attrs[0] = leaf->keys[i]; // key
				attrs[1] = leaf->idxpage[i]; // page_num
				attrs[2] = leaf->idxrec[i]; // rec_num
				rec.extractBPlusTreeNode(attrs);
				// if(i == 0 || i == numKeys-1) {
				// 	rec.show(schLeaf);
				// }
				AppendRecord(rec);
			}

			// in the end, finalize the page (=a node)
			WriteToFile(); // add pageNow to the file
			leaf = NULL;
		}
	}

	// free the memory
	delete [] attrs;

	// TestIndex();
}

void DBFile::InitBPlusTreeNodeSchema() {
	// create Schema for each type of record manually
	vector<string> attrs, types; vector<unsigned int> distincts;

	// schInterHeader
	attrs.push_back("is_leaf");
	attrs.push_back("left_most_ptr");
	attrs.push_back("num_recs");
	types.push_back("INTEGER"); types.push_back("INTEGER"); types.push_back("INTEGER");
	distincts.push_back(1); distincts.push_back(1); distincts.push_back(1);
	Schema sih(attrs, types, distincts);
	schInterHeader = sih;
	attrs.clear(); types.clear(); distincts.clear();
	
	// schInter
	attrs.push_back("key");
	attrs.push_back("ptr");
	attrs.push_back("is_duplicate");
	types.push_back("INTEGER"); types.push_back("INTEGER"); types.push_back("INTEGER");
	distincts.push_back(1); distincts.push_back(1); distincts.push_back(1);
	Schema si(attrs, types, distincts);
	schInter = si;
	attrs.clear(); types.clear(); distincts.clear();

	// schLeafHeader
	attrs.push_back("is_leaf");
	attrs.push_back("right_most_ptr");
	attrs.push_back("num_recs");
	types.push_back("INTEGER"); types.push_back("INTEGER"); types.push_back("INTEGER");
	distincts.push_back(1); distincts.push_back(1); distincts.push_back(1);
	Schema slh(attrs, types, distincts);
	schLeafHeader = slh;
	attrs.clear(); types.clear(); distincts.clear();

	// schLeaf
	attrs.push_back("key");
	attrs.push_back("page_num");
	attrs.push_back("rec_num");
	types.push_back("INTEGER"); types.push_back("INTEGER"); types.push_back("INTEGER");
	distincts.push_back(1); distincts.push_back(1); distincts.push_back(1);
	Schema sl(attrs, types, distincts);
	schLeaf = sl; 
	attrs.clear(); types.clear(); distincts.clear();
}

int DBFile::GetNext(Record& _fetchMe, int _lower, int _upper, DBFile& _heap) {
	int bitPos, key, pageNum, recNum;
	if(!isTreeTraversed) {
		MoveFirst(); isNewPage = true; 
		
		int isLeaf, edgePtr, numRecs = 0;
		int leftPtr, rightPtr, isDuplicate;
		int i = 0;

		Record rec;
		while(GetNext(rec) == 0) {
			char* bits = rec.GetBits();
			if(isNewPage) {
				bitPos = ((int *) bits)[1];
				isLeaf = *(int *)&(bits[bitPos]);
				bitPos = ((int *) bits)[2];
				edgePtr = *(int *)&(bits[bitPos]);
				bitPos = ((int *) bits)[3];
				numRecs = *(int *)&(bits[bitPos]);
				// if(isLeaf == 0)
				// 	rec.show(schInterHeader);
				// else
				// 	rec.show(schLeafHeader);
				isNewPage = false;
				i = 0;
			} else {
				if(isLeaf == 0) { // iNode
					// rec.show(schInter);
					bitPos = ((int *) bits)[1];
					key = *(int *)&(bits[bitPos]);
					if(i == 0) { leftPtr = edgePtr; }
					else { leftPtr = rightPtr; }
					bitPos = ((int *) bits)[2];
					rightPtr = *(int *)&(bits[bitPos]);
					bitPos = ((int *) bits)[3];
					isDuplicate = *(int *)&(bits[bitPos]);
					if(_lower < key || (_lower == key && isDuplicate == 1)) {
						GetPage(leftPtr); isNewPage = true;
					} else if(i == numRecs-1) { // last key in this node
						GetPage(rightPtr); isNewPage = true;
					} else { // continue to next key
						i++;
					}
				} else { // leaf
					// rec.show(schLeaf);
					bitPos = ((int *) bits)[1];
					key = *(int *)&(bits[bitPos]);
					bitPos = ((int *) bits)[2];
					pageNum = *(int *)&(bits[bitPos]);
					bitPos = ((int *) bits)[3];
					recNum = *(int *)&(bits[bitPos]);
					if((_lower < key  && key < _upper) ||
						(key == _lower && key == _upper)) {
						if(_heap.GetRecord(_fetchMe, pageNum, recNum) == 0) {
							treeNodePtr = iPage;
							isTreeTraversed = true;
							return 0;
						} else {
							cerr << "ERROR: Something wrong while GetRecord()" << endl << endl;
							return -1;
						}
					} else if(key <= _lower && key < _upper) { // this is because of duplicate
						continue; // until key becomes greater than _lower
					} else {
						return -1;
					}
				}
			}
		}
	} else { // at this point, we are in a leaf node
		Record rec;
		while(GetNext(rec) == 0) {
			if(isNewPage) { // leafHeader
				// rec.show(schLeafHeader);
				isNewPage = false;
				continue;
			} else { // others
				// rec.show(schLeaf);
			}
			char* bits = rec.GetBits();
			bitPos = ((int *) bits)[1];
			key = *(int *)&(bits[bitPos]);
			bitPos = ((int *) bits)[2];
			pageNum = *(int *)&(bits[bitPos]);
			bitPos = ((int *) bits)[3];
			recNum = *(int *)&(bits[bitPos]);

			if((_lower < key  && key < _upper) || (key == _lower && key == _upper)) {
				if(_heap.GetRecord(_fetchMe, pageNum, recNum) == 0) {
					treeNodePtr = iPage;
					return 0;
				} else {
					cerr << "ERROR: Something wrong while GetRecord()" << endl << endl;
					return -1;
				}
			} else {
				return -1;
			}
		}

		return -1;
	}
}

int DBFile::GetPage(off_t _whichPage) {
	if(!isMovedFirst) {
		MoveFirst();
	}

	off_t numPage = file.GetLength();
	if(_whichPage == numPage) {
		cerr << "ERROR: Index out of bound." << endl << endl;
		return -1;
	} else {
		iPage = _whichPage;
		// cout << "--- " << iPage << " ---" << endl;
		return file.GetPage(pageNow, _whichPage-1);		
	}
}

void DBFile::TestIndex() {
	MoveFirst(); isNewPage = true; 

	int isLeaf, edgePtr, numRecs = 0;
	int leftPtr, rightPtr, isDuplicate;
	int bitPos, key, pageNum, recNum;
	int i = 0;

	Record rec;
	while(GetNext(rec) == 0) {
		char* bits = rec.GetBits();
		if(isNewPage) {
			cout << "--- " << iPage << " ---" << endl;

			bitPos = ((int *) bits)[1];
			isLeaf = *(int *)&(bits[bitPos]);
			bitPos = ((int *) bits)[2];
			edgePtr = *(int *)&(bits[bitPos]);
			bitPos = ((int *) bits)[3];
			numRecs = *(int *)&(bits[bitPos]);			
			if(isLeaf == 0) {
				rec.show(schInterHeader);
			}	else {
				rec.show(schLeafHeader);
			}
			isNewPage = false;
			i = 0;
		} else {
			if(isLeaf == 0) { // iNode
				rec.show(schInter);
				if(i == numRecs-1) { // last key in this node
					isNewPage = true;					
				} else { // continue to next key
					i++;
				}
			} else { // leaf
				rec.show(schLeaf);
				if(i == numRecs-1) { // last key in this node
					isNewPage = true;					
				} else { // continue to next key
					i++;
				}				
			}
		}
	}
}
