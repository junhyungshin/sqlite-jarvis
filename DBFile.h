#ifndef DBFILE_H
#define DBFILE_H

#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "BPlusTree.h"

using namespace std;

class DBFile {
private:
	File file; // group of pages
	string fileName; // absolute path for the DBFile
	FileType fileType; // Heap, Sorted, Index (see Config.h)
	Page pageNow;
	off_t iPage; // index of the current page, pageNow, in file
	bool isMovedFirst;
	bool isTreeTraversed, isNewPage;
	int treeNodePtr;

	Schema schInterHeader, schInter, schLeafHeader, schLeaf;

public:
	DBFile ();
	virtual ~DBFile ();
	DBFile(const DBFile& _copyMe);
	DBFile& operator=(const DBFile& _copyMe);

	// creates a new heap file. FileType has to be Heap.
	// This is done only once, when a SQL table is created.
	int Create (char* fpath, FileType file_type);

	// gives access to the heap file.
	// The name is taken from the catalog, for every table
	int Open (char* fpath);

	// closes the file
	int Close ();

	// extracts records with a known schema from a text file passed as parameters.
	// Essentially, it converts the data from text to binary.
	// Method ExtractNextRecord from class Record does all the work for a given schema.
	void Load (Schema& _schema, char* textFile);

	// resets the file pointer to the beginning of the file, i.e., the first record
	void MoveFirst ();

	// appends the record passed as parameter to the end of the file.
	// This is the only method to add records to a heap file.
	void AppendRecord (Record& _addMe);

	// simply write the current page to the file
	// call this function to finalize appending records into a DBFile
	void WriteToFile();

	// returns the next record in the file.
	// The file pointer is moved to the following record
	// return 0 on success, -1 otherwise
	int GetNext (Record& _fetchMe);

	// get specified record from file
	// return 0 on success, -1 otherwise
	int GetRecord(Record& putItHere, off_t whichPage, off_t whichRecord);

	// this function retrieves tableName from fileName
	// defaultPath = "~/.sqlite-jarvis/heap/" + _table + ".dat" (see Catalog.cc)
	string GetTableName();

	// returns fileName
	const char* GetFileName();

	// simply returns the current page number
	int GetCurrentPageNum();

	// Functions below are about Index

	// load B+ tree and write into Index DBFile
	void LoadBPlusTree(BPlusTree& _tree);

	// create Schema for each type of node in B+ tree manually
	void InitBPlusTreeNodeSchema();

	// returns the next record in the leaf of B+ tree within (_lower, _upper)
	// (note that the range is non-inclusive because we don't take care of <= and >=)
	// at the very beginning, it starts to search from the root (done only once)
	// and once the pointer reaches at a leaf, 
	// it will return the record from _heap using GetRecord()
	// The file pointer is moved to the following record
	// return 0 on success, -1 otherwise
	int GetNext(Record& _fetchMe, int _lower, int _upper, DBFile& _heap);

	// get specific page
	// return 0 on success, -1 otherwise
	int GetPage(off_t _whichPage);

	void TestIndex();
};

#endif //DBFILE_H
