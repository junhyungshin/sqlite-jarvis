#include <iostream>
#include <algorithm>
#include <iterator>

#include "BPlusTree.h"

using namespace std;

BPlusTree::BPlusTree() {

  recs_per_inode = (PAGE_SIZE
                    - (1+3)*sizeof(int) // Positions in Record
                    - sizeof(int)       // #Total Records
                    - sizeof(int)       // Left Pointer to page
                    - sizeof(int))      // isLeaf
                    /
                    ( (1+3)*sizeof(int) // Positions in Record
                    + sizeof(int)       // Size of key
                    + sizeof(int)       // isDuplicate
                    + sizeof(int));     // Right Pointer to page

  recs_per_leaf = (PAGE_SIZE
                   - (1+3)*sizeof(int) // Positions in Record
                   - sizeof(int)       // #Total Records
                   - sizeof(int)       // Pointer to next page
                   - sizeof(int))      // isLeaf
                   /
                   ( (1+3)*sizeof(int) // Positions in Record
                   + sizeof(int)       // Size of key
                   + sizeof(int)       // Record Index
                   + sizeof(int));     // Page Index

  // Initialize root
  root = new LeafNode(recs_per_leaf);
  root->parent = NULL;
  LeafNode* rootleaf = (LeafNode*)root;
  rootleaf->next = NULL;

}

void BPlusTree::InsertKey(int key, int pageidx, int recidx) {

  //Find where to insert key in B+ tree
  int index;
  LeafNode* leaf = findKey(key, index);

  //insert key into leaf
  vector<int>::iterator it = leaf->keys.begin();
  leaf->keys.insert(it + index, key);
  it = leaf->idxrec.begin();
  leaf->idxrec.insert(it + index, recidx);
  it = leaf->idxpage.begin();
  leaf->idxpage.insert(it + index, pageidx);

  //split leaf node
  if (leaf->keys.size() > recs_per_leaf) {

    LeafNode* nleaf = new LeafNode(recs_per_leaf);

    //floor((n+1)/2)
    //Add 1 for move[lower, upper) and substract 1 as index starts from 0
    int split = (recs_per_leaf + 1)/2;

    //split keys and pointers and move them to nleaf
    copy(leaf->keys.begin() + split, leaf->keys.end(), back_inserter(nleaf->keys));
    leaf->keys.erase(leaf->keys.begin() + split, leaf->keys.end());
    copy(leaf->idxrec.begin() + split, leaf->idxrec.end(), back_inserter(nleaf->idxrec));
    leaf->idxrec.erase(leaf->idxrec.begin() + split, leaf->idxrec.end());
    copy(leaf->idxpage.begin() + split, leaf->idxpage.end(), back_inserter(nleaf->idxpage));
    leaf->idxpage.erase(leaf->idxpage.begin() + split, leaf->idxpage.end());

    nleaf->parent = leaf->parent;
    nleaf->next = leaf->next;
    leaf->next = nleaf;


    bool is_new_root_required = true;
    Node* lsibling = leaf;
    Node* rsibling = nleaf;
    key = rsibling->keys[0];

    //to keep track if the inserted node is duplicate
    bool isDuplicate = (lsibling->keys.back() == rsibling->keys[0]);

    //insert key in internal nodes
    while(lsibling->parent != NULL) {
      InternalNode* inode = (InternalNode*)lsibling->parent;

      //find position at which rsibling has to be inserted
      int idxlsib;
      int psize = inode->nodePtrs.size();
      if (inode->left == lsibling) idxlsib = -1;
      else {
        for (idxlsib = 0; \
          (idxlsib < psize) && (inode->nodePtrs[idxlsib] != lsibling);\
          idxlsib++);
      }

      //insert rsibling
      vector<int>::iterator itkeys = inode->keys.begin();
      inode->keys.insert(itkeys + idxlsib + 1, key);
      vector<Node*>::iterator itnodes = inode->nodePtrs.begin();
      inode->nodePtrs.insert(itnodes + idxlsib + 1, rsibling);
      vector<bool>::iterator itisd = inode->isDuplicate.begin();
      inode->isDuplicate.insert(itisd + idxlsib + 1, isDuplicate);

      //split internal nodes
      if (inode->keys.size() > recs_per_inode) {
        InternalNode* ninode = new InternalNode(recs_per_inode);

        lsibling = inode;
        rsibling = ninode;

        //Last element in lsibling will be inserted in parent
        split = recs_per_inode/2 + 1;

        //split keys and pointers and move them to ninode
        copy(inode->keys.begin() + split, inode->keys.end(), back_inserter(ninode->keys));
        inode->keys.erase(inode->keys.begin() + split, inode->keys.end());
        copy(inode->nodePtrs.begin() + split, inode->nodePtrs.end(), back_inserter(ninode->nodePtrs));
        inode->nodePtrs.erase(inode->nodePtrs.begin() + split, inode->nodePtrs.end());
        copy(inode->isDuplicate.begin() + split, inode->isDuplicate.end(), back_inserter(ninode->isDuplicate));
        inode->isDuplicate.erase(inode->isDuplicate.begin() + split, inode->isDuplicate.end());

        //All nodePtrs in ninode need to change their parent
        int nisize = ninode->keys.size();
        for(int i = 0; i < nisize; i++) {
          ninode->nodePtrs[i]->parent = ninode;
        }

        //last element of lsibling becomes left of rsibling, and key to be
        //inserted in parent in next loop
        ninode->left = inode->nodePtrs.back();
        inode->nodePtrs.pop_back();
        key = inode->keys.back();
        inode->keys.pop_back();
        isDuplicate = inode->isDuplicate.back();
        inode->isDuplicate.pop_back();

        ninode->parent = inode->parent;

      }
      else {
        is_new_root_required = false;
        break;
      }
    }

    //make new root
    if (is_new_root_required) {
      InternalNode* nroot = new InternalNode(recs_per_inode);

      nroot->parent = NULL;
      nroot->left = lsibling;
      nroot->keys.push_back(key);
      nroot->nodePtrs.push_back(rsibling);
      nroot->isDuplicate.push_back(isDuplicate);

      lsibling->parent = nroot;
      rsibling->parent = nroot;

      root = nroot;
    }
  }
}

bool BPlusTree::GetNext(Node*& node) {
  if(isFirst) {
    //Prepare for BFS
    curLevel.push_back(root);
    idxNode = 0;
    isFirst = false;
  }
  if (idxNode < curLevel.size()) {
    node = curLevel[idxNode];
    idxNode++;
    return true;
  }
  else {

    if (curLevel[0]->isLeaf) return false;
    else {
      vector<Node*> nlevel;
      int clsize = curLevel.size();
      for(int i = 0; i < clsize; i++) {

        InternalNode* inode = (InternalNode*) curLevel[i];
        nlevel.push_back(inode->left);
        int npsize = inode->nodePtrs.size();

        for(int j = 0; j < npsize; j++) {
          nlevel.push_back(inode->nodePtrs[j]);
        }
      }

      curLevel = nlevel;
      node = curLevel[0];
      idxNode = 1;
      return true;
    }
  }
}

LeafNode* BPlusTree::findKey(int key, int& index) {
  Node* node = root;
  index = -1;
  while (!node->isLeaf) {
    InternalNode* inode = (InternalNode*) node;

    //Get index of value just lesser than key
    // int idx = GetIndex(key, inode->keys) - 1;
    int size = inode->keys.size();
    int i;
    for(i = 0; i < size; i++) {
      if (inode->keys[i] > key) {
        //Go to left child
        node = (i == 0) ? inode->left : inode->nodePtrs[i-1];
        break;
      }
      else if (inode->keys[i] == key) {
        if (inode->isDuplicate[i]) {
          //Go to left child
          node = (i == 0) ? inode->left : inode->nodePtrs[i-1];
        }
        else {
          //Go to right child
          node = inode->nodePtrs[i];
        }
        break;
      }
    }

    if (i == size) {
      //Go to last child
      node = inode->nodePtrs[i-1];
    }

    // //size of vector = 0
    // if (idx == -1) {
    //   return NULL;
    // }

    //TODO - check this part for both inode and leaf
    // if (inode->isDuplicate[idx]) {
    //   //Go to the left child of present key
    //   //For the first key, left child is inode->left
    //   node = (idx == 0) ? inode->left : inode->nodePtrs[idx-1];
    // }
    // else {
    //   //Go to the right child of present key
    //   node = inode->nodePtrs[idx];
    // }
  }

  //Get index of value just lesser than key
  index = GetIndex(key, node->keys);

  LeafNode* retNode = (LeafNode *)node;
  return retNode;
}

int BPlusTree::GetIndex(int key, vector<int>& keys) {
  int size = keys.size();

  // //size of vector = 0
  // if (size == 0) return -1;

  //search for value just greater than key
  int i;
  for(i = 0; (keys[i] < key) && (i < size); i++);

  return i;
}
