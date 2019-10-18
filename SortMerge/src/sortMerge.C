
#include <string.h>
#include <assert.h>
#include "sortMerge.h"
#include <map>
// Error Protocall:

enum ErrCodes
{
  SORT_FAILED,
  HEAPFILE_FAILED
};

static const char *ErrMsgs[] = {
    "Error: Sort Failed.",
    "Error: HeapFile Failed."
    // maybe more ...
};

static error_string_table ErrTable(JOINS, ErrMsgs);
#define MAX_Length 128
#define MAX_Tuple 100
struct rec
{
  int key;
  char filler[4];
};
struct rec1
{
  char filler[4];
  int key;
};

int SortPage(char *filename1, char *outputfile, int len_in1, AttrType in1[], short t1_str_sizes[], int join_col_in1, TupleOrder order);

// sortMerge constructor
sortMerge::sortMerge(
    char *filename1,      // Name of heapfile for relation R
    int len_in1,          // # of columns in R.
    AttrType in1[],       // Array containing field types of R.
    short t1_str_sizes[], // Array containing size of columns in R
    int join_col_in1,     // The join column of R

    char *filename2,      // Name of heapfile for relation S
    int len_in2,          // # of columns in S.
    AttrType in2[],       // Array containing field types of S.
    short t2_str_sizes[], // Array containing size of columns in S
    int join_col_in2,     // The join column of S

    char *filename3,  // Name of heapfile for merged results
    int amt_of_mem,   // Number of pages available
    TupleOrder order, // Sorting order: Ascending or Descending
    Status &s         // Status of constructor
)
{

#if 1
  char sort_filename2[] = "sort2", sort_filename1[] = "sort1";
  Sort(filename1, sort_filename1, len_in1, in1, t1_str_sizes, join_col_in1, order, amt_of_mem, s);
  SortPage(filename1, sort_filename1, len_in1, in1, t1_str_sizes, join_col_in1, order); // sorted R

  //       HeapFile *f_in2=new HeapFile(sort_filename2,s);
  Sort(filename2, sort_filename2, len_in2, in2, t2_str_sizes, join_col_in2, order, amt_of_mem, s);
  SortPage(filename2, sort_filename2, len_in2, in2, t2_str_sizes, join_col_in2, order); // sorted S

  Status hp;
  HeapFile *f_in2 = new HeapFile(sort_filename2, s);
  HeapFile *f_in1 = new HeapFile(sort_filename1, s);
  HeapFile *f_out = new HeapFile(filename3, hp);

#endif
  short key_begin1 = 0, key_begin2 = 0;
  for (short i = 0; i < join_col_in1; i++)
    key_begin1 = key_begin1 + t1_str_sizes[i]; // get the begin address of key1

  for (short i = 0; i < join_col_in2; i++)
    key_begin2 = key_begin2 + t2_str_sizes[i]; // get the begin address of key2

  // init tumcmp key position
  key1_pos = 0;
  key2_pos = 0;

  short length1 = 0, length2 = 0;
  for (short i = 0; i < len_in1; i++)
    length1 += t1_str_sizes[i]; // get total length of tuple 1

  for (short i = 0; i < len_in2; i++)
    length2 += t2_str_sizes[i]; // get total length of tuple 2
  void *t1;
  void *t2;
  t1 = malloc(t1_str_sizes[join_col_in1]);
  t2 = malloc(t1_str_sizes[join_col_in1]);
  void *recPtr1 = malloc(length1);
  void *recPtr2 = malloc(length2);
  char temp[1024];
  rec rec_arry1[MAX_Tuple], rec_arry2[MAX_Tuple];
  int j = 0, len;
  RID rid, rid_insert, first;
  Scan *scan2 = f_in2->openScan(hp);
  if (hp != OK)
    cout << "Fail: open scan 2" << endl;
  Scan *scan1 = f_in1->openScan(hp);
  if (hp != OK)
    cout << "Fail: open scan 1" << endl;
  Status HP_INSERT, scan_all1, scan_all2;
  scan_all1 = scan1->getNext(rid, (char *)recPtr1, len);
  scan_all2 = scan2->getNext(rid, (char *)recPtr2, len);
  first = rid; // get first record of a page

  HP_INSERT = OK;
  while (HP_INSERT == OK && scan_all1 == OK)
  {

    memcpy(t1, recPtr1 + key_begin1, t1_str_sizes[join_col_in1]);

    if (scan2->position(first) != OK)
      cout << "Error: can not reach the position" << endl;
    scan_all2 = scan2->getNext(rid, (char *)recPtr2, len); // scan S, find the match tuple
    while (scan_all2 == OK)
    {

      memcpy(t2, recPtr2 + key_begin2, t2_str_sizes[join_col_in2]);
      //   test code  cout<<"insert t1 "<<*(int *)t1<<"   t2 "<<*(int *)t2<<" tumcmp="<<tupleCmp(t1,t2)<<" keybigin1 "<<key_begin1<<" keybig2  "<<key_begin2<<endl;
      if (tupleCmp(t1, t2) == 0) //find match
      {
        memcpy(temp, recPtr1, length1);
        memcpy(temp + length1, recPtr2, length2);
        HP_INSERT = f_out->insertRecord(temp, length1 + length2, rid); // insert match tuple
        if (HP_INSERT != OK)
          cout << "this is ridiculous !" << endl;
      }
      else if (tupleCmp(t1, t2) < 0) // R tuple K less than S, stop scan
        break;
      j++;
      scan_all2 = scan2->getNext(rid, (char *)recPtr2, len);
    }
    j = 0;
    scan_all1 = scan1->getNext(rid, (char *)recPtr1, len);
  }

  delete scan1, scan2;
  s = f_in1->deleteFile();
  s = f_in2->deleteFile();
  delete f_out, f_in1;
  delete f_in2;
  s = OK;

  // fill in the body
}

// sortMerge destructor
sortMerge::~sortMerge()
{
  // fill in the body
}

int SortPage(char *filename1, char *outputfile, int len_in1, AttrType in1[], short t1_str_sizes[], int join_col_in1, TupleOrder order)
{
  // init tumcmp compare key
  key1_pos = 0;
  key2_pos = 0;
  Status hp, sort_read;
  HeapFile *f = new HeapFile(filename1, hp);
  HeapFile *f_out = new HeapFile(outputfile, hp);
  if (hp != OK)
  {
    cout << "Failed: open heapfile " << filename1 << endl;
  }

  short key_begin = 0;
  for (short i = 0; i < join_col_in1; i++)
    key_begin = key_begin + t1_str_sizes[i]; // get the begin address of key

  short length = 0;
  for (short i = 0; i < len_in1; i++)
    length += t1_str_sizes[i]; // get total length of tuple

  void *t1;
  void *t2;
  t1 = malloc(t1_str_sizes[join_col_in1]);
  t2 = malloc(t1_str_sizes[join_col_in1]);
  void *recPtr = malloc(length);
  char rec_arry[MAX_Tuple][MAX_Length];
  memset(rec_arry, 0, MAX_Tuple * MAX_Length);
  int k = 0, curr_page = 0, len;
  RID rid, rid_insert, first;
  Scan *scan = f->openScan(hp);
  if (hp != OK)
    cout << "Fail: open scan " << endl;

  // get first rid of a page
  scan->getNext(rid, (char *)recPtr, len);
  first = rid;
  curr_page = rid.pageNo;
  if (scan->position(first) != OK)
    cout << "Error: can not reach the position" << endl;
  // get whole record of page, then sort
  while (scan->getNext(rid, (char *)recPtr, len) == OK && curr_page == rid.pageNo)
  {

    memcpy(rec_arry[k], recPtr, len);
    memcpy(t1, rec_arry[k] + key_begin, t1_str_sizes[join_col_in1]);
    k++;
  }

  Page *sort_slot = new Page();
  sort_read = MINIBASE_DB->read_page(curr_page, sort_slot);
  HFPage *sort_hp = (HFPage *)sort_slot;

#if 1
  // quick sort , for slot offset arry
  int key, j;
  char temp[MAX_Length];
  for (int i = 1; i < k; i++)
  {

    memcpy(t1, rec_arry[i] + key_begin, t1_str_sizes[join_col_in1]);
    memcpy(temp, rec_arry[i], length);
    j = i - 1;
    memcpy(t2, rec_arry[j] + key_begin, t1_str_sizes[join_col_in1]);

    while (j >= 0 && tupleCmp(t2, t1) > 0)
    {
      //   cout<<"key sort ="<<*(int *)t1<<"   "<<*(int *)t2<<endl;
      memcpy(rec_arry[j + 1], rec_arry[j], length);
      j--;
      memcpy(t2, rec_arry[j] + key_begin, t1_str_sizes[join_col_in1]);
    }

    memcpy(rec_arry[j + 1], temp, length);
  }

  sort_hp->init(curr_page); // clear current page, update new sort order

  // init output heapfile, set page number
  Page *sort_slot1 = new Page();
  Scan *scan_out = f_out->openScan(hp);
  scan_out->getNext(rid, (char *)recPtr, len);
  int out_pageno = rid.pageNo;
  sort_read = MINIBASE_DB->read_page(out_pageno, sort_slot1);
  HFPage *out_sort = (HFPage *)sort_slot1;
  out_sort->init(out_pageno);
  // update a page with sorted order

  // insert sort order to orignal file and output heap file
  for (int i = 0; i < k; i++)
  {
    memcpy(t1, rec_arry[i] + key_begin, t1_str_sizes[join_col_in1]);
    out_sort->insertRecord(rec_arry[i], length, rid_insert);
    sort_hp->insertRecord(rec_arry[i], length, rid_insert);
  }

  // write back to disk
  sort_read = MINIBASE_DB->write_page(curr_page, sort_slot);
  sort_read = MINIBASE_DB->write_page(out_pageno, sort_slot1);

  delete scan, sort_slot1;

  delete f, f_out;
  delete sort_slot, sort_slot1;
#endif

  return 1;
}
