/*
 * implementation of class Scan for HeapFile project.
 * $Id: scan.C,v 1.1 1997/01/02 12:46:42 flisakow Exp $
 */

#include <stdio.h>
#include <stdlib.h>

#include "heapfile.h"
#include "scan.h"
#include "hfpage.h"
#include "buf.h"
#include "db.h"


PageId First_DataPage;
//int Final_page=0;
Page *P_pin=new Page();
HFPage *Page_data=new HFPage();
  struct rec {
         int key;
         char    filler[4];
      };
// *******************************************
// The constructor pins the first page in the file
// and initializes its private data members from the private data members from hf
Scan::Scan (HeapFile *hf, Status& status)
{ 

     First_DataPage=this->dataPageId=hf->firstDirPageId+2;  
     this->dirPageId=hf->firstDirPageId;
     this->_hf=hf;
     Status  get_status=MINIBASE_BM->pinPage(First_DataPage,P_pin,1,_hf->fileName);
      if(get_status!=OK)
       cout<<"Error: pin a first data pageID "<<First_DataPage<<endl;
       init(hf);
      status = OK;
}

// *******************************************
// The deconstructor unpin all pages.
Scan::~Scan()
{

    // MINIBASE_BM->flushAllPages();
  #if 0
    FrameDesc *del_pin=MINIBASE_BM->frameTable();
    int pin_number=MINIBASE_BM->getNumUnpinnedBuffers();
    int pin_un=MINIBASE_BM->getNumBuffers();
    Replacer *A;
    for(int i=0;i<=pin_number;i++)  {}

    //  A->info();
     pin_number=MINIBASE_BM->getNumUnpinnedBuffers();
     cout<<"number of frame pin ="<<pin_number<<"  number of unpin ="<<pin_un<<endl;
  //  delete P_pin;
 //   delete Page_data;
#endif
  // put your code here
}

// *******************************************
// Retrieve the next record in a sequential scan.
// Also returns the RID of the retrieved record.
Status Scan::getNext(RID& rid, char *recPtr, int& recLen)
{

   if(this->dataPage->empty()==true)  { rid=this->userRid;  return DONE;  }
    RID next=(this->userRid);
    RID nextRid;
    RID rid_fisrt_datePage;
    PageId  next_page;
  //   cout<<"page number"<<next.pageNo<<"slot number ="<<next.slotNo<<endl;
    Status get_status=(this->dataPage)->getRecord(next,recPtr,recLen);  // get record
 //    Status get_status=(this->dataPage)->returnRecord(next,recPtr,recLen);  // get record

    if(get_status!=OK)
      cout<<"Error: can not get recpter for pageid= "<<next.pageNo<<"slot numner="<<next.slotNo<<endl;
     rid=next;
     get_status=(this->dataPage)->nextRecord(next,nextRid);   // get next rid
    if(get_status==OK)    
     {
      this->userRid=nextRid; 
    //  cout<<"next record page no"<<nextRid.pageNo<<endl;
     }
    else if(get_status==DONE)
    {
         get_status=MINIBASE_BM->unpinPage(this->dataPageId,FALSE,_hf->fileName);   // unpin current page 
         if(get_status!=OK)
          cout<<"Warning: cannot (scan)  unpin page ="<<this->dataPageId<<endl;
         get_status=nextDataPage();     // get next page
        if(get_status==DONE)
        {  
           get_status=MINIBASE_BM->pinPage(this->dataPageId,P_pin,1,_hf->fileName);   // pin the final data page
          if(get_status!=OK)
           cout<<"Warning: cannot pin page ="<<this->dataPageId<<endl;
           Final_page++;       // if Final_page=1 mean final recod ,so cannot return done. only wait it >=2 can do return done
          if(Final_page>=2)   // final record read by function getRecod 
          {
          
            get_status=MINIBASE_BM->unpinPage(this->dataPageId,FALSE,_hf->fileName);  // unpin final page
             if(get_status!=OK)
             cout<<"Warning: cannot unpin page ="<<this->dataPageId<<endl;
             Final_page=0;
             return DONE;    
         }


      }
    }
    else
      return FAIL;
  // put your code here
  return OK;
}

// *******************************************
// Do all the constructor work.
Status Scan::init(HeapFile *hf)
{

   // data page init
     RID rid_fisrt_datePage;
     Page *P_pin1=new Page();
     HFPage *Page_data1=new HFPage();
 
     Status P_page=MINIBASE_DB->read_page(First_DataPage,P_pin1);
  
     if(P_page!=OK)
     { 
      cout<<"Error: cann not read page "<<this->dataPageId<<"into the memory"<<endl;
      return FAIL;
      }
      memcpy(Page_data1,P_pin1,sizeof(Page));
      this->dataPage=Page_data1;
      Page_data1->firstRecord(rid_fisrt_datePage);
      this->userRid=rid_fisrt_datePage;

 
#if 1
// Dir page init
      Page *P_dir=new Page();
      HFPage *Page_data_dir=new HFPage();
      Status  get_status=MINIBASE_BM->pinPage(this->dirPageId,P_pin,1,_hf->fileName);
      if(get_status!=OK)
        cout<<"Warning: cannot pin a first directory pageID "<<this->dirPageId<<endl;
       P_page=MINIBASE_DB->read_page(this->dirPageId,P_dir);
      if(P_page!=OK)
     { 
      cout<<"Error: cann not read page "<<this->dirPageId<<"into the memory"<<endl;
      return FAIL;
      }
       memcpy(Page_data_dir,P_dir,sizeof(Page));
       this->dirPage=Page_data_dir;
       Page_data_dir->firstRecord(rid_fisrt_datePage);
       this->dataPageRid=rid_fisrt_datePage;
    // upin first dir page
        get_status=MINIBASE_BM->unpinPage(this->dirPageId,FALSE,_hf->fileName);
         if(get_status!=OK)
          cout<<"Warning: unpin page ="<<this->dirPageId<<endl;

        delete P_dir,Page_data1;
        delete Page_data_dir,P_pin1;
#endif
  // put your code here
    return OK;
}

// *******************************************
// Reset everything and unpin all pages.
Status Scan::reset()
{
   

    
  // put your code here
  return OK;
}

// *******************************************
// Copy data about first page in the file.
Status Scan::firstDataPage()
{

      RID rid_fisrt_datePage;
     Status get_status=MINIBASE_BM->pinPage(First_DataPage,P_pin,1,_hf->fileName);
      if(get_status!=OK)
      cout<<"Warning:cannot pin page ="<<First_DataPage<<endl;
      Status P_page=MINIBASE_DB->read_page(First_DataPage,P_pin);
     if(P_page!=OK)
     { 
      cout<<"Error: cann not read page "<<First_DataPage<<"into the memory"<<endl;
      return FAIL;
     }
      memcpy(Page_data,P_pin,sizeof(Page));
      Page_data->firstRecord(rid_fisrt_datePage);
      this->dataPage=Page_data;
      this->userRid=rid_fisrt_datePage;
      this->dataPageId=Page_data->page_no();
  // put your code here
       return OK;
}

// *******************************************
// Retrieve the next data page.
Status Scan::nextDataPage(){

     RID rid_next_datePage;
     PageId next_data_page;
     next_data_page=(this->dataPage)->getNextPage(); // get next pageid
     if(next_data_page<0)
      { 
   
        return DONE;
      }
         
    Status get_status=MINIBASE_BM->pinPage(next_data_page,P_pin,1,_hf->fileName);
      if(get_status!=OK)
      cout<<"Warning:cannot  (next data page)pin page ="<<next_data_page<<endl;

     Status P_page=MINIBASE_DB->read_page(next_data_page,P_pin);
     if(P_page!=OK)
     {
      cout<<"Error: cann not read page "<<next_data_page<<"into the memory"<<endl;
      return FAIL;
    }
      memcpy(Page_data,P_pin,sizeof(Page));
      Page_data->firstRecord(rid_next_datePage);   // get first rid
      this->dataPage=Page_data;                  // get hfpage address
      this->userRid=rid_next_datePage;           // get current rid 
      this->dataPageId=Page_data->page_no();   // get pageid
  
     return OK;
}

// *******************************************
// Retrieve the next directory page.
Status Scan::nextDirPage() {

    Page *P_dir=new Page();
     HFPage *Page_data_dir=new HFPage();
     RID rid_next_DirPage;
     PageId next_dir_page;
     next_dir_page=(this->dirPage)->getNextPage();
     if(next_dir_page<0)
      { 
        cout<<"Error: no directory page any more"<<endl;
        return DONE;
      }     
       Status get_status=MINIBASE_BM->pinPage(next_dir_page,P_dir,1,_hf->fileName);
      if(get_status!=OK)
      cout<<"Warning: cannot pin page ="<<next_dir_page<<endl;
     Status P_page=MINIBASE_DB->read_page(next_dir_page,P_pin);
     if(P_page!=OK)
      { 
        cout<<"Error: cann not read page "<<next_dir_page<<"into the memory"<<endl;
        return FAIL;
       }
      memcpy(Page_data_dir,P_dir,sizeof(Page));
      Page_data_dir->firstRecord(rid_next_DirPage);
      this->dirPage=Page_data_dir;
      this->dataPageRid=rid_next_DirPage;
      this->dirPageId=Page_data_dir->page_no();

         delete P_dir;
        delete Page_data_dir;
  // put your code here
  return OK;
}


// *******************************************
// Position the scan cursor to the record with the given rid.
// Returns OK if successful, non-OK otherwise.
Status Scan::position(RID rid)
{
  //  cout<<"Call  position"<<endl;
     RID rid_fisrt_datePage;
       Status   get_status=MINIBASE_BM->pinPage(this->dataPageId,P_pin,1,_hf->fileName);   // pin the final data page
          if(get_status!=OK)
           cout<<"Warning: cannot pin page ="<<this->dataPageId<<endl;
      this->dataPage->firstRecord(rid_fisrt_datePage);
      RID next;
      while(this->dataPage->nextRecord(rid_fisrt_datePage,next)==OK)
      {
        if(rid_fisrt_datePage==rid)
        {
          this->userRid=rid_fisrt_datePage;
           Final_page=0;
           return OK;

        }
        rid_fisrt_datePage=next;
      }
      
   
   // put your code here
   return OK;
}

// *******************************************
// Move to the next record in a sequential scan.
// Also returns the RID of the (new) current record.
Status Scan::mvNext(RID& rid)
{
//   cout<<"Call  MvNext  "<<endl;
    RID next;
  if(this->dataPage->nextRecord(userRid,next)==OK)
    rid=next;
  else
    cout<<"Error: Can not get next record "<<endl;


    // put your code here
  return OK;
}
