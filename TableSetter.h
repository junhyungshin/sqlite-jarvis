#ifndef _TABLE_SETTER_H
#define _TABLE_SETTER_H

#include <regex>

#include "Catalog.h"
#include "RelOp.h"

using namespace std;


class TableSetter {
private:
	Catalog* catalog;

public:
	TableSetter(Catalog& _catalog);
	~TableSetter();

	void createTable(char* _table, AttrAndTypeList* _attrAndTypes);
	void loadData(char* _table, char* _textFile);
	void dropTable(char* _table);
	void createIndex(char* _index, char* _table, char* _attr);
	void dropIndex(char* _index);
};

#endif // _TABLE_SETTER_H
