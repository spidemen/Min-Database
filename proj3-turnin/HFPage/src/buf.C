/*****************************************************************************/
/*************** Implementation of the Buffer Manager Layer ******************/
/*****************************************************************************/

#include "buf.h"
#include <list>
#include <vector>
#include <algorithm>
#include <stack>
#include <deque>
#include <queue>
#include <math.h>
// Define buffer manager error messages here
//enum bufErrCodes  {...};

typedef struct LinkList
{
  int PageId;
  int frameID;
} * List;
typedef list<LinkList> *Linkhash;
#define INT_MAX 4294967200
#define BuckSize 2
vector<Linkhash> hash_table(8, NULL);
int a = 1, b = 0;
int Next = 0, level = 2;
int partion_flag = 1;
int hashbuf = HTSIZE + 1;
void hash_build(PageId PageNo, int frameNo);
void hash_remove(int page);
int hash_search(int pageID, int &frameNo);
void print_hash();
void Hash_delte();
vector<PageId> disk_page;
stack<int> Hated_Frame;
queue<int> Loved_Frame;
vector<int> copy_stack;
int flag_buf_full;
// Define error message here
static const char *bufErrMsgs[] = {
    // error message strings go here
    "Not enough memory to allocate hash entry",
    "Inserting a duplicate entry in the hash table",
    "Removing a non-existing entry from the hash table",
    "Page not in hash table",
    "Not enough memory to allocate queue node",
    "Poping an empty queue",
    "OOOOOOPS, something is wrong",
    "Buffer pool full",
    "Not enough memory in buffer manager",
    "Page not in buffer pool",
    "Unpinning an unpinned page",
    "Freeing a pinned page"};

// Create a static "error_string_table" object and register the error messages
// with minibase system
static error_string_table bufTable(BUFMGR, bufErrMsgs);
//*************************************************************
//** This is the implementation of BufMgr
//************************************************************

BufMgr::BufMgr(int numbuf, Replacer *replacer)
{

  Page *BufPage = new Page[numbuf];
  FrameDesc *BufDesript = new FrameDesc[numbuf];
  this->bufPool = BufPage;
  this->bufDescr = BufDesript;
  this->numBuffers = -1; // init 0   it from biggest number ++ become 0
                         //   cout<<"bufmgr "<<this->numBuffers<<endl;
  while (!Hated_Frame.empty())
    Hated_Frame.pop();
  while (!Loved_Frame.empty())
    Loved_Frame.pop();
  Hash_delte();
  init_frame(-1);
  flag_buf_full = 0;

  //    cout<<"bufmgr "<<this->numBuffers<<endl;
  // put your code here
}

//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr()
{

  if (this->numBuffers > INT_MAX)
    this->numBuffers++;
  // cout<<"number of frame="<<this->numBuffers<<endl;
  int i = 0;
  while (i <= this->numBuffers)
  {
    if (this->bufDescr[i].dirtybit == true)
    {
      //   cout<<"write page to disk"<<endl;
      Page *replace = new Page();
      memcpy(replace, &this->bufPool[i], sizeof(Page));
      Status buf_write = MINIBASE_DB->write_page(this->bufDescr[i].pageNo, replace); //write disk
      disk_page.push_back(this->bufDescr[i].pageNo);
      if (buf_write != OK)
        cout << "Error: write buf page " << this->bufDescr[i].pageNo << "into to disk" << endl;
    }
    i++;
  }

  // put your code here
}

//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage)
{
  int frame;
  //  cout<<"frame number  pin without filename"<<this->numBuffers<<"page number="<<PageId_in_a_DB<<endl;
  if (!hash_search(PageId_in_a_DB, frame) && this->numBuffers == (NUMBUF - 1))
  {
    int i = 0;
    //  cout<<"size of hate "<<Hated_Frame.size()<<endl;
    if (!Hated_Frame.empty())
    {
      //    cout<<"size of hate "<<Hated_Frame.size()<<endl;
      //    cout<<"size of Love "<<Loved_Frame.size()<<endl;
      i = Hated_Frame.top();
      Hated_Frame.pop();
    }
    else if (!Loved_Frame.empty())
    {
      //    cout<<"size of Love "<<Loved_Frame.size()<<endl;
      i = Loved_Frame.front();
      Loved_Frame.pop();
    }

    //   while(this->bufDescr[i].pin_cnt!=0&&i<20) i++;    // find replace one
    //  if(i==20) { cout<<"Fata Error: do not find pin_cnt=0 in the buf "<<endl; return FAIL; }
    if (this->bufDescr[i].dirtybit == true)
    {
      Page *replace = new Page();
      memcpy(replace, &this->bufPool[i], sizeof(Page));
      Status buf_write = MINIBASE_DB->write_page(this->bufDescr[i].pageNo, replace); //write disk
      disk_page.push_back(PageId_in_a_DB);
      if (buf_write != OK)
        cout << "Error: write buf page " << this->bufDescr[i].pageNo << "into to disk" << endl;
    }
    //  cout<<"remove page "<<this->bufDescr[i].pageNo<<endl;
    hash_remove(this->bufDescr[i].pageNo); // remove from hash table

    Page *replace = new Page();
    Status buf_read = MINIBASE_DB->read_page(PageId_in_a_DB, replace);
    if (buf_read == OK)
    {
      memcpy(&this->bufPool[i], replace, sizeof(Page));
      page = &this->bufPool[i];
      this->bufDescr[i].pageNo = PageId_in_a_DB;
      this->bufDescr[i].pin_cnt = 1;
      this->bufDescr[i].dirtybit = false;
    }
    else
    {
      //   cout<<"Fata error: can not read page in the disk"<<endl;
      return FAIL;
    }

    hash_build(PageId_in_a_DB, i); // insert new record into hash table
                                   //               flag_buf_full=1;
  }
  else if (!hash_search(PageId_in_a_DB, frame) && this->numBuffers < (NUMBUF - 1) || this->numBuffers > INT_MAX)
  {
    //  if(this->numBuffers>INT_MAX) cout<<"biggerst number enter " <<endl;
    Page *replace = new Page();
    Status buf_read = MINIBASE_DB->read_page(PageId_in_a_DB, replace);
    if (buf_read == OK)
    {
      this->numBuffers++;
      memcpy(&this->bufPool[this->numBuffers], replace, sizeof(Page));
      page = &this->bufPool[this->numBuffers]; // allocate into buf
      this->bufDescr[this->numBuffers].pageNo = PageId_in_a_DB;
      this->bufDescr[this->numBuffers].pin_cnt++;
      this->bufDescr[this->numBuffers].dirtybit = false;
      hash_build(PageId_in_a_DB, this->numBuffers); // insert into hash table
      if (this->numBuffers == (NUMBUF - 1))
        flag_buf_full = 1;
    }
    else
    {
      //detect a error code
      // cout<<"Error: can not read page from disk"<<endl;
      return FAIL;
      /*   this->numBuffers++;
              page=&this->bufPool[this->numBuffers];      // allocate into buf
             this->bufDescr[this->numBuffers].pageNo=PageId_in_a_DB;
             this->bufDescr[this->numBuffers].pin_cnt++;
             this->bufDescr[this->numBuffers].dirtybit=false;
              hash_build(PageId_in_a_DB,this->numBuffers);   // insert
              */
    }
  }
  else if (hash_search(PageId_in_a_DB, frame))
  {
    this->bufDescr[frame].pin_cnt++;
    page = &this->bufPool[frame];
  }
  else
    cout << "can not pin this page  " << endl;

  //   print_hash();
  // put your code here
  return OK;
} //end pinPage

void hash_build(PageId PageNo, int frameNo)
{

  // test insert page a=2; b=1;

  int Max_next = BuckSize * pow(2, level) - 1;
  int index = (a * PageNo + b) % hashbuf; //key
  LinkList frame;
  frame.PageId = PageNo;
  frame.frameID = frameNo;

  if (!hash_table[index])
  {
    list<LinkList> *buck = new list<LinkList>;
    buck->push_back(frame);
    hash_table[index] = buck; // point to the buck
  }
  else
  {

    list<LinkList> *buck = hash_table[index];
    if (buck->size() < BuckSize)
      buck->push_back(frame); // insert into the buck
    else
    {
      if (partion_flag || Max_next == Next)
      {
        if (Max_next == Next)
        {
          level++;
          hashbuf = 2 * hashbuf;
        } // parition when next equal to Max_next
        hash_table.resize(2 * (hashbuf), NULL);
        partion_flag = 0; // first parition flag
      }
      int hash_size = (hashbuf)*2;               // double length of hash table
      int index1 = (a * PageNo + b) % hash_size; // find new index for insert record
      int partion_index;
      list<LinkList>::iterator it = buck->begin();
      if (index1 <= Next)
      {
        int overflow = 0;
        while (it != buck->end())
        {
          //  cout<<"pade id "<<(*it).PageId<<endl;
          partion_index = (*it).PageId;
          partion_index = (a * partion_index + b) % hash_size; // find new index for insert record
          if (index != partion_index)                          // if not the same , insert into new buck
          {
            LinkList frame1;
            frame1.PageId = (*it).PageId;
            frame1.frameID = (*it).frameID;
            if (!hash_table[partion_index])
            {
              list<LinkList> *buck1 = new list<LinkList>;
              buck1->push_back(frame1);
              hash_table[partion_index] = buck1;
            }
            else
              hash_table[partion_index]->push_back(frame1); // the same  , make a copy
            it = buck->erase(it);                           // delete copy  if the same, if not , delete it
            overflow = 1;
          }

          it++;
        }

        if (!hash_table[index1]) // find new index for new insert reocrd
        {
          list<LinkList> *buck2 = new list<LinkList>;
          buck2->push_back(frame);
          hash_table[index1] = buck2;
        }
        else
          hash_table[index1]->push_back(frame);
        if (!overflow)
          Next++; // next move
      }
      else
      {
        buck->push_back(frame); // overflow
      }
    }
  }
}
void print_hash()
{
  cout << "size of hash buf " << hashbuf << " size of  hash" << hash_table.size() << "next =" << Next << endl;
  for (int i = 0; i < hash_table.size(); i++)
  {

    if (hash_table[i])
    {
      list<LinkList> *buck = hash_table[i];
      if (!hash_table[i])
        return;
      cout << "hash key  " << i << endl;
      list<LinkList>::iterator it = buck->begin();
      while (it != buck->end())
      {
        cout << "  page id=" << (*it).PageId << endl;
        it++;
      }
    }
  }
}
void hash_remove(int page)
{
  int index = (a * page + b) % hashbuf; //key
  list<LinkList> *buck = hash_table[index];
  list<LinkList>::iterator it = buck->begin();
  while (it != buck->end())
  {
    if ((*it).PageId == page)
    {
      buck->erase(it);
      return;
    }
    it++;
  }

  index = (a * page + b) % (2 * hashbuf); //key
  if (index <= hash_table.size())
  {
    buck = hash_table[index];
    it = buck->begin();
    while (it != buck->end())
    {
      if ((*it).PageId == page)
      {
        buck->erase(it);
        break;
      }
      it++;
    }
  }
}

int hash_search(int pageID, int &frameNo)
{
  int index = (a * pageID + b) % hashbuf; //key
  if (!hash_table[index])
    return 0;
  list<LinkList> *buck = hash_table[index];
  list<LinkList>::iterator it = buck->begin();
  while (it != buck->end())
  {
    if ((*it).PageId == pageID)
    {
      frameNo = (*it).frameID;
      return 1;
    }
    it++;
  }
  index = (a * pageID + b) % (2 * hashbuf); //key
  if (index <= hash_table.size())
  {
    if (!hash_table[index])
      return 0;
    buck = hash_table[index];
    it = buck->begin();
    while (it != buck->end())
    {
      if ((*it).PageId == pageID)
      {
        frameNo = (*it).frameID;
        return 1;
      }
      it++;
    }
  }
  return 0;
};

void Hash_delte()
{
#if 1
  for (int index = 0; index < hash_table.size(); index++)
  {
    if (!hash_table[index])
      continue;
    list<LinkList> *buck = hash_table[index];
    hash_table[index] = NULL;
    buck->~list<LinkList>();
  }
  Next = 0;
  level = 2;
  partion_flag = 1;
  hashbuf = HTSIZE + 1;
#endif

  // hash_table(7,NULL);
}
//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId page_num, int dirty = FALSE, int hate = FALSE)
{

  int frameid;
  if (hash_search(page_num, frameid))
  {
    if (this->bufDescr[frameid].pin_cnt == 0)
    {
      return FAIL;
    }
    this->bufDescr[frameid].pin_cnt--;
    this->bufDescr[frameid].dirtybit = dirty;
    if (this->bufDescr[frameid].pin_cnt == 0)
    {
      if (hate == FALSE)
      {
        Loved_Frame.push(frameid);
      }
      else
      {
        Hated_Frame.push(frameid);
      }
    }
    //   cout<<"unpin a page  pageid="<<page_num<<endl;
  }
  else
  {
    //   cout<<"can not find the page in the buf pool pageid="<<page_num<<endl;
    return FAIL;
  }
  // put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of newPage
//************************************************************
Status BufMgr::newPage(PageId &firstPageId, Page *&firstpage, int howmany)
{
  int allocate_page;
  Page *new_page = new Page();
  Status allocte, get_new, deallocte, pin;
  howmany = 1;
  allocte = MINIBASE_DB->allocate_page(allocate_page, howmany);
  if (allocte != OK)
    cout << "Error: can not allocate a page from DB" << endl;
  get_new = MINIBASE_DB->read_page(allocate_page, new_page);
  //  if(this->numBuffers>=(NUMBUF-1))
  if (flag_buf_full)
  {
    deallocte = MINIBASE_DB->deallocate_page(allocate_page, howmany);
    if (deallocte != OK)
      cout << "Fata Error: can not dellocate page  pageid" << allocate_page << endl;
    return FAIL;
  }
  else
  {
    pin = pinPage(allocate_page, new_page, 1);
    firstPageId = allocate_page;
    firstpage = new_page;
  }
  //   firstPageId=allocate_page;
  //   firstpage=new_page;
  // put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
Status BufMgr::freePage(PageId globalPageId)
{
  int frame;
  if (hash_search(globalPageId, frame))
  {
    if (this->bufDescr[frame].pin_cnt)
      return FAIL;
    else
    {
      int i = frame + 1;
      while (i <= this->numBuffers)
      {

        memcpy(&this->bufDescr[frame], &this->bufDescr[i], sizeof(FrameDesc));
        frame++;
        i++;
      }
      this->numBuffers--;
    }
  }
  Status deallocte = MINIBASE_DB->deallocate_page(globalPageId);
  if (deallocte != OK)
    cout << "ERROR: can not dellocate a page  pageid=" << globalPageId << endl;

  // put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of flushPage
//************************************************************
Status BufMgr::flushPage(PageId pageid)
{

  int frameid;
  if (hash_search(pageid, frameid))
  {
    Page *replace = new Page();
    memcpy(replace, &this->bufPool[frameid], sizeof(Page));
    Status buf_write = MINIBASE_DB->write_page(pageid, replace); //write disk
    disk_page.push_back(pageid);
    if (buf_write != OK)
    {
      cout << "Error: write buf page " << this->bufDescr[frameid].pageNo << "into to disk" << endl;
      return FAIL;
    }
  }
  else
    cout << "can not find the page in the buf pool pageid=" << pageid << endl;

  // put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
Status BufMgr::flushAllPages()
{

  int i = 0;
  if (this->numBuffers > INT_MAX)
    this->numBuffers++;
  while (i <= this->numBuffers)
  {
    if (this->bufDescr[i].dirtybit == true)
    {
      //   cout<<"write page to disk"<<endl;
      Page *replace = new Page();
      memcpy(replace, &this->bufPool[i], sizeof(Page));
      Status buf_write = MINIBASE_DB->write_page(this->bufDescr[i].pageNo, replace); //write disk
      disk_page.push_back(this->bufDescr[i].pageNo);
      if (buf_write != OK)
        cout << "Error: write buf page " << this->bufDescr[i].pageNo << "into to disk" << endl;
    }
    i++;
  }

  //put your code here
  return OK;
}

/*** Methods for compatibility with project 1 ***/
//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage, const char *filename)
{

  int frame;
  //  cout<<"file pin pade id"<<PageId_in_a_DB<<" frame "<<this->numBuffers<<endl;

  if (!hash_search(PageId_in_a_DB, frame) && this->numBuffers == (NUMBUF - 1))
  {

    int i;
    //    cout<<"size of hate "<<Hated_Frame.size()<<endl;
    if (!Hated_Frame.empty())
    {
      //  cout<<"size of hate "<<Hated_Frame.size()<<endl;
      //    cout<<"size of Love "<<Loved_Frame.size()<<endl;
      i = Hated_Frame.top();
      //     cout<<"size of hate "<<Hated_Frame.size()<<" i="<<i<<endl;
      Hated_Frame.pop();
      copy_stack.erase(copy_stack.end() - 1);
    }
    else if (!Loved_Frame.empty())
    {
      //    cout<<"size of Love "<<Loved_Frame.size()<<endl;
      i = Loved_Frame.front();
      Loved_Frame.pop();
    }
    //    while(this->bufDescr[i].pin_cnt!=0&&i<20) i++;    // find replace one
    //   if(i==20) { cout<<"Fata Error: do not find pin_cnt=0 in the buf "<<endl; return FAIL; }
    if (this->bufDescr[i].dirtybit == true)
    {
      Page *replace = new Page();
      memcpy(replace, &this->bufPool[i], sizeof(Page));
      Status buf_write = MINIBASE_DB->write_page(this->bufDescr[i].pageNo, replace); //write disk
      disk_page.push_back(PageId_in_a_DB);
      if (buf_write != OK)
        cout << "Error: write buf page " << this->bufDescr[i].pageNo << "into to disk" << endl;
    }
    //  cout<<"remove page "<<this->bufDescr[i].pageNo<<endl;
    //  cout<<"remove page "<<this->bufDescr[i].pageNo<<endl;
    hash_remove(this->bufDescr[i].pageNo); // remove from hash table
    Page *replace = new Page();
    Status buf_read = MINIBASE_DB->read_page(PageId_in_a_DB, replace);
    if (buf_read == OK)
    {
      memcpy(&this->bufPool[i], replace, sizeof(Page));
      page = &this->bufPool[i];
      this->bufDescr[i].pageNo = PageId_in_a_DB;
      this->bufDescr[i].pin_cnt = 1;
      this->bufDescr[i].dirtybit = false;
    }
    else
    {
      cout << "Fata error: can not read page in the disk" << endl;
      return FAIL;
    }

    hash_build(PageId_in_a_DB, i); // insert new record into hash table
  }

#if 1
  else if ((!hash_search(PageId_in_a_DB, frame) && this->numBuffers < (NUMBUF - 1)) || this->numBuffers > INT_MAX)
  {
    //  if(this->numBuffers>INT_MAX) cout<<"biggerst number enter  pageid= " <<PageId_in_a_DB<<endl;
    Page *replace = new Page();
    Status buf_read = MINIBASE_DB->read_page(PageId_in_a_DB, replace);
    if (buf_read == OK)
    {
      this->numBuffers++;
      if (emptyPage)
        memcpy(&this->bufPool[this->numBuffers], replace, sizeof(Page));
      page = &this->bufPool[this->numBuffers]; // allocate into buf
      this->bufDescr[this->numBuffers].pageNo = PageId_in_a_DB;
      this->bufDescr[this->numBuffers].pin_cnt++;
      this->bufDescr[this->numBuffers].dirtybit = false;
      hash_build(PageId_in_a_DB, this->numBuffers); // insert into hash table
                                                    // cout<<"page "<<PageId_in_a_DB<<" pin_cnt "<<this->bufDescr[this->numBuffers].pin_cnt<<endl;
      if (this->numBuffers == (NUMBUF - 1))
        flag_buf_full = 1;
    }
    else
    {
      //detect a error code
      cout << "Error: can not read page from disk" << endl;
      return FAIL;
      /*   this->numBuffers++;
              page=&this->bufPool[this->numBuffers];      // allocate into buf
             this->bufDescr[this->numBuffers].pageNo=PageId_in_a_DB;
             this->bufDescr[this->numBuffers].pin_cnt++;
             this->bufDescr[this->numBuffers].dirtybit=false;
              hash_build(PageId_in_a_DB,this->numBuffers);   // insert
              */
    }
  }
  else if (hash_search(PageId_in_a_DB, frame))
  {
    this->bufDescr[frame].pin_cnt++;
    page = &this->bufPool[frame];
    //   this->bufDescr[frame].pageNo=PageId_in_a_DB;
    //  this->bufDescr[frame].dirtybit=false;
  }
  else
    cout << "can not pin this page  " << endl;

    //   print_hash();

#endif

#if 0
         else if(!hash_search(PageId_in_a_DB,frame))
          {
             cout<<"max test"<<"pageid="<<PageId_in_a_DB<<endl;
               Page *replace=new Page();
              Status buf_read=MINIBASE_DB->read_page(PageId_in_a_DB,replace);

            this->numBuffers++;
            page=&this->bufPool[this->numBuffers];
            memcpy(&this->bufPool[this->numBuffers],replace,sizeof(Page));
            this->bufDescr[this->numBuffers].pageNo=PageId_in_a_DB;
            this->bufDescr[this->numBuffers].pin_cnt++;
            this->bufDescr[this->numBuffers].dirtybit=false;
            hash_build(PageId_in_a_DB,this->numBuffers); 
          }
         else 
         {
          //  this->numBuffers++;
             page=&this->bufPool[frame];      // allocate into buf
            this->bufDescr[frame].pageNo=PageId_in_a_DB;
            this->bufDescr[frame].pin_cnt++;
            this->bufDescr[frame].dirtybit=false;
           // hash_build(PageId_in_a_DB,this->numBuffers);   // insert into hash table
         }
#endif

  //put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename)
{

  int frameid;
  if (hash_search(globalPageId_in_a_DB, frameid))
  {

    if (this->bufDescr[frameid].pin_cnt == 0)
    {
      cout << "unpind page_cnt=0. pagde id=" << globalPageId_in_a_DB << endl;
      return FAIL;
    }
    this->bufDescr[frameid].pin_cnt--;
    if (this->bufDescr[frameid].pin_cnt == 0 && find(copy_stack.begin(), copy_stack.end(), frameid) == copy_stack.end())
    {
      Hated_Frame.push(frameid);
      copy_stack.push_back(frameid);
    }
    //    cout<<"unpin file "<<globalPageId_in_a_DB<<" pin_cnt"<<this->bufDescr[frameid].pin_cnt<<endl;
  }
  else
  {
    // cout<<"can not find the page in the buf pool pageid="<<globalPageId_in_a_DB<<endl;
    return FAIL;
  }

  //put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
//************************************************************
unsigned int BufMgr::getNumUnpinnedBuffers()
{

  int i = 0;
  int count = 0;
  if (this->numBuffers > INT_MAX)
    this->numBuffers++;
  while (i <= this->numBuffers)
  {
    if (!this->bufDescr[i].pin_cnt)
      count++;
    i++;
  }
  //put your code here
  return count;
}
