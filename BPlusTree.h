#ifndef _B_PLUS_TREE_H
#define _B_PLUS_TREE_H

#include <vector>

#include "Config.h"

using namespace std;

//struct from which all nodes are inherited
struct Node {

  // All the keys in the Node
  vector<int> keys;

  // To determine if the Node is an Internal Node or Leaf Node
  bool isLeaf;

  //Pointer to parent
  Node* parent;
};

//struct representing an internal node
struct InternalNode : Node {
  // Pointers to other Internal Nodes
  // At index = i, right child of key at index = i is stored
  vector<Node*> nodePtrs;

  // Leftmost Pointer not paired up with any key
  Node* left;

  // Determine if key is Duplicate
  vector<bool> isDuplicate;

  //Constructor
  InternalNode(int recs_per_inode) {
    keys.reserve(recs_per_inode + 1);
    nodePtrs.reserve(recs_per_inode + 1);
    isDuplicate.reserve(recs_per_inode + 1);
    isLeaf = false;
    left = NULL;
  }
};

//struct representing a leaf node
struct LeafNode : Node {

  // Indices of records in _dbfile page
  vector<int> idxrec;

  // Indices of pages in _dbfile
  vector<int> idxpage;

  // Pointer to next LeafNode
  LeafNode* next;

  //Constructor
  LeafNode(int recs_per_leaf) {
    keys.reserve(recs_per_leaf + 1);
    idxrec.reserve(recs_per_leaf + 1);
    idxpage.reserve(recs_per_leaf + 1);
    isLeaf = true;
    next = NULL;
  }
};

class BPlusTree {
public:

  //Constructor - calculates recs_per_leaf and recs_per_inode
  BPlusTree();

  // Inserts a single record in B+ tree
  void InsertKey(int key, int pageidx, int recidx);

  // Gives the next Node of the tree while performing a BFS
  bool GetNext(Node*& node);

private:

  //First call to GetNext
  bool isFirst = true;

  // Root Node
  Node* root;

  // Number of records per Internal Node and Leaf Node
  int recs_per_inode, recs_per_leaf;

//Used for BFS
  //nodes at current level
  vector<Node*> curLevel;
  //idx of current node in curLevel
  int idxNode;

  // finds and returns first occurence of key (if found) or key just
  // greater than given key along with the corresponding LeafNode
  LeafNode* findKey(int key, int& index);

  // finds and returns first occurence of key (if found) or key just
  // greater than given key from a vector of keys
  int GetIndex(int key, vector<int>& keys);

};

#endif
