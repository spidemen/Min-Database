#include "heapfile.h"

// ******************************************************
// Error messages for the heapfile layer

#define Slot_Size 4       // slot size
#define HP_FILE_SIZE 8   // size of each headfile ,which contain 400 pages
#define HF_Dir_SIZE 2         // total directory page of a headfile
int flag_lock=0;          // record 1st insert for datapage
int flag_dir=0;           //record 1st insert for dirpage
PageId  page_id,page_id_pre,page_id_dir;
HFPage *Page_pin=new HFPage();
HFPage *Page_Dir=new HFPage();
int cout_record;        //number of records of a headfile
RID  Rid_dir;


struct _rec1 {
  char  filler[4];
  int key;
};


static const char *hfErrMsgs[] = {
    "bad record id",
    "bad record pointer", 
    "end of file encountered",
    "invalid update operation",
    "no space on page for record", 
    "page is empty - no records",
    "last record on page",
    "invalid slot number",
    "file has already been deleted",
};

static error_string_table hfTable( HEAPFILE, hfErrMsgs );

// ********************************************************
// Constructor
HeapFile::HeapFile( const char *name, Status& returnStatus )
{
    
     PageId start_pg;
     int len=strlen(name)+1;
    char  *File=new char[len];
     memset(File,0,len);
     strcpy(File,name);
   if(MINIBASE_DB->get_file_entry(name,start_pg)==OK)
     {       
      // return the dir pageID and filename. pageid for datapage  page_id_dir for dirpages
         this->firstDirPageId=start_pg;
         this->fileName=File;
         page_id=page_id_pre;     // datapageID from last current datapage
         page_id_dir=this->firstDirPageId;    // dirpage ID
         returnStatus = OK; 
     }
     else
     {
      // initialize parameter
       Status pageStat=MINIBASE_DB->allocate_page(start_pg,HP_FILE_SIZE);
       if(pageStat==OK)
        pageStat=MINIBASE_DB->add_file_entry(name,start_pg);   // add entry 
      else
        cout<<"Fail to allocate  page "<<endl;
       if(pageStat!=OK)
        cout<<"Error: Cannot add file entry "<<name<<"  start page number "<<start_pg<<endl;
        this->firstDirPageId=start_pg;
        this->fileName=File;    // directory page
        page_id=this->firstDirPageId+HF_Dir_SIZE;  //first  data page 
        page_id_dir=this->firstDirPageId;
        flag_lock=flag_dir=0;     
        cout_record=0;

       Page *del_m=new Page();
       HFPage  *init_data=(HFPage *)del_m;
       init_data->init(page_id);
       MINIBASE_DB->write_page(page_id,del_m);   // clear first data page which is dirty before
       delete del_m;
    //   cout<<"test create heapfile start  name "<<name<<" data page no "<<this->firstDirPageId+HF_Dir_SIZE<<endl;    // test code
        if(MINIBASE_DB->get_file_entry(name,start_pg)==OK)
         returnStatus = OK;
        else
        { 
          cout<<"Error: Cannot get file entry "<<name<<endl;
          returnStatus= FAIL;
         }

    }
    // fill in the body
   //   returnStatus = OK;
   
}

// ******************
// Destructor
HeapFile::~HeapFile()
{

      memset(Page_pin,0,1024);

     //delete Page_pin,Page_Dir;
  //   Page_pin=Page_Dir=NULL;
   // fill in the body 

}

// *************************************
// Return number of records in heap file
int HeapFile::getRecCnt()
{
 
    return cout_record;
  
   // return OK;
}

// *****************************
// Insert a record into the file
Status HeapFile::insertRecord(char *recPtr, int recLen, RID& outRid)
{

     Status  P_page,P_insert;
     DataPageInfo  *Insert_Dir=new DataPageInfo();
     Page *P_pin=new Page();
     int cout_Page_record=0;
     PageId  allocDirPageId,nextPageID;
     RID allocDataPageRid;
     Status  get_status;
    if(flag_lock==0)
    {
   // insert 1st record 
      page_id=this->firstDirPageId+HF_Dir_SIZE;
  
        Page_pin->init(page_id);
        P_insert=Page_pin->insertRecord(recPtr,recLen,outRid);
       if(P_insert!=OK)
        cout<<"Warnning:cannot insert first record "<<endl;
        cout_record++;
        cout_Page_record++;   //
        flag_lock=1;
         memcpy(P_pin,Page_pin,sizeof(HFPage));
         P_page=MINIBASE_DB->write_page(page_id,P_pin);  //write back
         if(P_page!=OK)
          cout<<"Error: cann not write page "<<page_id<<"into the memory"<<endl;

        //  cout<<"First test insert  pageid  "<<page_id<<endl;   // test code
    }
    else
    {
    
      //  cout<<"second test insert  pageid  "<<page_id<<"  len="<<recLen<<"  key="<<(*((struct _rec1*)recPtr)).key<<endl;    // test code
      // insert record 
        P_insert=Page_pin->insertRecord(recPtr,recLen,outRid);
        if(P_insert!=OK&&P_insert!=DONE)
        cout<<"Warnning: cannot insert record "<<endl;
        cout_record++;
        cout_Page_record++;
        //write back to the disk
   
         memcpy(P_pin,Page_pin,sizeof(HFPage));
          P_page=MINIBASE_DB->write_page(page_id,P_pin);  //write back
         if(P_page!=OK)
          cout<<"Error: cann not write page "<<page_id<<"into the memory"<<endl;
     //     get_status=MINIBASE_BM->unpinPage(page_id,FALSE,this->fileName);

       if(P_insert!=OK&&Page_pin->available_space()<=(recLen+4))    // seek for the next page if current full
       {

          nextPageID=page_id+1;
          Page_pin->setNextPage(nextPageID);     // setting for pages 
      //    get_status=MINIBASE_BM->pinPage(page_id,P_pin,1,this->fileName);
          memcpy(P_pin,Page_pin,sizeof(HFPage));     
          P_page=MINIBASE_DB->write_page(page_id,P_pin);   //write data back 
         if(P_page!=OK)
          cout<<"Error: cann not write page "<<page_id<<"into the memory"<<endl;
  #if 1      
          else
          {
            //initialisze parameter for Insert_Dir
            Insert_Dir->pageId=page_id;
            Insert_Dir->availspace=Page_pin->available_space();
            Insert_Dir->recct=cout_Page_record;
            cout_Page_record=0;
            this->allocateDirSpace(Insert_Dir,allocDirPageId,allocDataPageRid);   // put this information to the directory pages
        
          }
  #endif

            page_id++;
            Page_pin->init(page_id);
           Page_pin->setPrevPage(page_id-1);
            page_id_pre=page_id;
            P_insert=Page_pin->insertRecord(recPtr,recLen,outRid);
            if(P_insert!=OK)      // cannot insert to a new page, too big
             {         
               MINIBASE_FIRST_ERROR( HEAPFILE, NO_SPACE);
               return HEAPFILE;
              }
          memcpy(P_pin,Page_pin,sizeof(HFPage));
          P_page=MINIBASE_DB->write_page(page_id,P_pin);   // write data back 
         if(P_page!=OK)
          cout<<"Error: cann not write page "<<page_id<<"into the memory"<<endl;   
          flag_lock=1;
       //    get_status=MINIBASE_BM->unpinPage(page_id,FALSE,this->fileName);
       }

       

    }

    delete P_pin;

    return OK;
} 

// ***********************
// delete record from file
Status HeapFile::deleteRecord (const RID& rid)
{
    PageId datapage=rid.pageNo;
    short slot_offset=rid.slotNo;
     Page *P_del=new Page();
      HFPage *Page_pin=new HFPage();
     HFPage *Page_Dir=new HFPage();
     Status P_page;;
     P_page=MINIBASE_DB->read_page(datapage,P_del);   //read page to memory
    if(P_page!=OK)
    cout<<"Error: cann not read  data page "<<page_id<<"into the memory"<<endl;
   memcpy(Page_pin,P_del,sizeof(Page));
   P_page=Page_pin->deleteRecord(rid);         // delete it 
   if(P_page!=OK)
   {
    cout<<"Error: delete record  pageid="<<datapage<<" offset slot="<<slot_offset<<endl;
    return DONE;
   }
    memcpy(P_del,Page_pin,sizeof(HFPage));        //write back 
    P_page=MINIBASE_DB->write_page(datapage,P_del);
   if(P_page!=OK)
           cout<<"Error: cann not write back data page "<<page_id<<"into the memory"<<endl;
    delete P_del,Page_pin,Page_Dir;
    cout_record--;     // number of record should reduce 1  
  // fill in the body
   return OK;
}

// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord (const RID& rid, char *recPtr, int recLen)
{
   
     PageId datapage=rid.pageNo;
     short slot_offset=rid.slotNo;
     Page *P_update=new Page();
     Status P_page;;
     char *temp_recptr;
     int length=recLen;

      PageId  Dir_page,dataPage;
       RID rid_data;
       HFPage *Page_pin=new HFPage();
        HFPage *Page_Dir=new HFPage();
// get data page and dir page
     Status  get_status=findDataPage(rid,Dir_page,Page_Dir,dataPage,Page_pin,rid_data);
    if(get_status!=OK)
      { 
        cout<<"Error: cannot get data page and dirctory page"<<endl;
        return FAIL;
        }
   P_page=Page_pin->returnRecord(rid,temp_recptr,recLen);   // get  record address
   if(P_page!=OK)
    cout<<"Error: get record for update  pageid="<<rid.pageNo<<" Slotno="<<rid.slotNo<<endl;
    if(length!=recLen)
     {
        return MINIBASE_FIRST_ERROR( HEAPFILE, INVALID_UPDATE );
     }
//update record
   memcpy(temp_recptr,recPtr,recLen);
   memcpy(P_update,Page_pin,sizeof(HFPage));

   P_page=MINIBASE_DB->write_page(datapage,P_update);   //write back
   if(P_page!=OK)
       cout<<"Error: cann not write  data page "<<page_id<<"into the memory"<<endl;
     get_status=MINIBASE_BM->unpinPage(dataPage,FALSE,this->fileName);   //upin data page


     delete P_update,Page_pin,Page_Dir;

  //   cout<<"test buf update file.  dir page ="<<Dir_page<<". data page "<<dataPage<<endl;

  // fill in the body
  return OK;
}

// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord (const RID& rid, char *recPtr, int& recLen)
{
    PageId datapage=rid.pageNo;
     short slot_offset=rid.slotNo;
     Page *P_get=new Page();
     Status P_page;

      PageId  Dir_page,dataPage;
      RID rid_data;
        HFPage *Page_pin=new HFPage();
        HFPage *Page_Dir=new HFPage();
  // get data page and dir page
    Status  get_status=findDataPage(rid,Dir_page,Page_Dir,dataPage,Page_pin,rid_data);
    if(get_status!=OK)
      { 
        cout<<"Error: cannot get data page and dirctory page"<<endl;
        return FAIL;
        }
    else
    {
      P_page=Page_pin->getRecord(rid,recPtr,recLen);   // get record from hpage
     if(P_page!=OK)
      cout<<"Error: Cann not get record from page "<<datapage<<endl;
 //     get_status=MINIBASE_BM->unpinPage(Dir_page,FALSE,this->fileName);
      get_status=MINIBASE_BM->unpinPage(dataPage,FALSE,this->fileName);    //unpin data page

      return OK;
    }

    delete P_get,Page_pin,Page_Dir;
  //   exit(0);
  // fill in the body 
  
}

// **************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status& status)
{
    Page *P_pin;
    HeapFile hf(this->fileName,status);  
    Scan  *H_File_scan= new Scan(&hf,status);  
    return H_File_scan;    // return class scan 
  // fill in the body 
  
}

// ****************************************************
// Wipes out the heapfile from the database permanently. 
Status HeapFile::deleteFile()
{
    Status del;
    del=MINIBASE_DB->delete_file_entry(this->fileName);   // delete file entry
    if(del!=OK)
      {
       cout<<"Error: del file entry  "<<this->fileName<<endl;
       return FILEEOF;
       }

     for(int i=this->firstDirPageId+HF_Dir_SIZE;i<=this->firstDirPageId+HF_Dir_SIZE;i++)
     {
      Page *del_m=new Page();
   //   del=MINIBASE_DB->write_page(i,del_m); 
   //   if(del!=OK) cout<<"Error: wipe a page "<<endl;
      delete del_m; 
    }
   
    del=MINIBASE_DB->deallocate_page(this->firstDirPageId,HP_FILE_SIZE);    // erase pages

    if(del!=OK)
     { 
      cout<<"Error: del deallocate page    heapfilen="<<this->fileName<<"from  "<<this->firstDirPageId<<"size  "<<HP_FILE_SIZE<<endl;
       return FILEEOF;
     }
    // fill in the body
    return OK;
}

// ****************************************************************
// Get a new datapage from the buffer manager and initialize dpinfo
// (Allocate pages in the db file via buffer manager)
Status HeapFile::newDataPage(DataPageInfo *dpinfop)
{
    // current do not used in this project , next project would fill up this part
    // fill in the body
    return OK;
}

// ************************************************************************
// Internal HeapFile function (used in getRecord and updateRecord): returns
// pinned directory page and pinned data page of the specified user record
// (rid).
//
// If the user record cannot be found, rpdirpage and rpdatapage are 
// returned as NULL pointers.
//
Status HeapFile::findDataPage(const RID& rid,
                    PageId &rpDirPageId, HFPage *&rpdirpage,
                    PageId &rpDataPageId,HFPage *&rpdatapage,
                    RID &rpDataPageRid)
{
     PageId page_dir, page_data,first_dir_page;
     first_dir_page=this->firstDirPageId;
     Page *P_find=new Page();
     Status P_page,P_rid,get_status;
     PageId Current_page=rid.pageNo;
     short slot_offset=rid.slotNo;
     char *recPtr;
     int recLen;
  // rid.pageNo=-1 mean , insert operation, rid.slotNo= length of record needed to insert
    if(rid.pageNo==-1)       // insert , search a page which have available space for insertion 
    {
         int flag_dir_finish=0;
     while(!flag_dir_finish&&first_dir_page<=(this->firstDirPageId+HF_Dir_SIZE)&&first_dir_page!=-1) 
     {
         get_status=MINIBASE_BM->pinPage(first_dir_page,P_find,1,this->fileName);
        P_page=MINIBASE_DB->read_page(first_dir_page,P_find);
         if(P_page!=OK)
          { 
            cout<<"Error: cann not read  directory page "<<page_id<<"into the memory"<<endl;
             return FAIL;
           }
          memcpy(Page_pin,P_find,sizeof(Page));
          RID  first,next;
          P_rid=Page_pin->firstRecord(first);   //get first rid
          if(P_rid!=OK)
          {
            cout<<"Warnning: can not get first rid  pageno="<<first_dir_page<<" slot ="<<first.slotNo<<endl;
            return FAIL;
 
          }
          // if 1st record insert
          if(Page_pin->empty()<=0||Page_pin->page_no()==0)
          {
            rpDirPageId=page_dir=first_dir_page;
            rpDataPageId=first_dir_page+8;
            rpdirpage=Page_pin;
            P_page=MINIBASE_DB->read_page(rpDataPageId,P_find);
            memcpy(Page_pin,P_find,sizeof(HFPage));
            if(P_page!=OK)
            cout<<"Error: cann not read  data page "<<page_id<<"into the memory"<<endl;
            rpdatapage=Page_pin;
            rpDataPageRid.pageNo=rpDataPageId;
            rpDataPageRid.slotNo=0;
          }
          else
          {
            // other record insert, find the datapage whose available space big the record len plus sizeof slot
           while(Page_pin->nextRecord(first,next)==OK)
            {
               P_rid=Page_pin->returnRecord(first,recPtr,recLen);
              if(P_rid!=OK)
               cout<<"Warnning: can not get first record from  pageno="<<first_dir_page<<" slot ="<<first.slotNo<<endl;
              DataPageInfo * dpinfop=new DataPageInfo();
              if(recLen!=sizeof(DataPageInfo))  
              cout<<"Error: get DataPageInfo from hpage pageid="<<Current_page<<" slot number"<<first.slotNo<<endl;
              memcpy(dpinfop,recPtr,recLen);
              // search in a dir page 
             if(dpinfop->availspace>(rid.slotNo+4))  // find the space , return datapage and pageid
                {
                 flag_dir_finish=1;
                 rpdirpage=Page_pin;
                 rpDirPageId=Page_pin->page_no();
                 break;
                 }
               first=next;
             }
             // next dir pages
            if(!flag_dir_finish)
             {  
                 get_status=MINIBASE_BM->unpinPage(first_dir_page,FALSE,this->fileName);
                 first_dir_page=Page_pin->getNextPage();   
             }
          }
      }
    }
    // rid.pageNo!=-1 mean no insert ,just get dir and page
    else              // find data page and directory page
    {

        int flag_dir_finish=0;
      while(!flag_dir_finish&&first_dir_page<=(this->firstDirPageId+HF_Dir_SIZE)&&first_dir_page!=-1)    // find dir page 
      {
     
    //     get_status=MINIBASE_BM->pinPage(first_dir_page,P_find,1,this->fileName);
         P_page=MINIBASE_DB->read_page(first_dir_page,P_find);
        if(P_page!=OK)
          { 
            cout<<"Error: cann not read  directory page "<<page_id<<"into the memory"<<endl;
             return FAIL;
           }
       //   get_status=MINIBASE_BM->pinPage(first_dir_page,P_find,1,this->fileName);
          memcpy(Page_pin,P_find,sizeof(Page));
          RID  first,next;
          P_rid=Page_pin->firstRecord(first);
          if(P_rid!=OK)
          {
            cout<<"Warnning: can not get first rid  pageno="<<first_dir_page<<" slot ="<<first.slotNo<<endl;
            return FAIL;
 
          }
      
           // search in a dir page 
          while(Page_pin->nextRecord(first,next)==OK)
          {
               P_rid=Page_pin->returnRecord(first,recPtr,recLen);
              if(P_rid!=OK)
               cout<<"Warnning: can not get first record from  pageno="<<first_dir_page<<" slot ="<<first.slotNo<<endl;
              DataPageInfo * dpinfop=new DataPageInfo();
              if(recLen!=sizeof(DataPageInfo))  
              cout<<"Error: get DataPageInfo from hpage pageid="<<Current_page<<" slot number"<<first.slotNo<<endl;
              memcpy(dpinfop,recPtr,recLen);
              if(dpinfop->pageId==Current_page)   //find datapage, return 
               {
                 flag_dir_finish=1;
                 rpdirpage=Page_pin;
                 rpDirPageId=Page_pin->page_no();
                 if(rpDirPageId!=first_dir_page)
                  cout<<"Error: the dir pageID isconsistent with readpage "<<endl;
                 break;
               }
              first=next;
          }
        //    PageId testpin=first_dir_page;
        //   first_dir_page=Page_pin->getNextPage(); 
           if(!flag_dir_finish&&first_dir_page!=-1)
            {  
            //  cout<<"test for getdatapage pin"<<endl;
         //     get_status=MINIBASE_BM->unpinPage(testpin,FALSE,this->fileName);
               first_dir_page=Page_pin->getNextPage();     // go to the next page
            }
        }

        if(!flag_dir_finish) rpdirpage=NULL;  // dir page do not exit

        // find data page
         get_status=MINIBASE_BM->pinPage(Current_page,P_find,1,this->fileName);
         P_page=MINIBASE_DB->read_page(Current_page,P_find);
        if(P_page!=OK)
          { 
             cout<<"Error: cann not read  page "<<Current_page<<"into the memory"<<endl;
             rpdatapage=NULL;
             return FAIL;
          }
          // return pageid and rpdatapage.....
         memcpy(Page_pin,P_find,sizeof(Page));
         rpdatapage=Page_pin;
         rpDataPageId=Page_pin->page_no();
         rpDataPageRid=rid;
    

    }

 //   delete P_find;
    // fill in the body
    return OK;
}

// *********************************************************************
// Allocate directory space for a heap file page 

Status HeapFile::allocateDirSpace(struct DataPageInfo * dpinfop,
                            PageId &allocDirPageId,
                            RID &allocDataPageRid)
{
    DataPageInfo  Dir_page;
    Page *P_pin=new Page();
 //   HFPage *Page_Dir=new HFPage();
    Status P_insert,P_page;
  //  allocDirPageId=this->firstDirPageId;
    Dir_page.pageId=dpinfop->pageId;
    Dir_page.availspace=dpinfop->availspace;
    Dir_page.recct=dpinfop->recct;
  // create dir pages
    if(!flag_dir)
    {
        Page_Dir->init(page_id_dir);
        P_insert=Page_Dir->insertRecord((char *)dpinfop,sizeof(DataPageInfo),Rid_dir);    //insert 1st to dir page
        if(P_insert!=OK)
          cout<<"Error: insert first directory record "<<endl;
        allocDataPageRid=Rid_dir;
        flag_dir=1;
    }
    else
    {
        P_insert=Page_Dir->insertRecord((char *)dpinfop,sizeof(DataPageInfo),Rid_dir);  
         if(P_insert!=OK&&Page_Dir->available_space()<=(sizeof(DataPageInfo)+4))    //current page full, then allocate next page
         {
        //   Page *P_pin=new Page();
           memcpy(P_pin,Page_Dir,sizeof(HFPage));
           P_page=MINIBASE_DB->write_page(page_id_dir,P_pin);
          if(P_page!=OK)
           cout<<"Error: cann not write  directory page "<<page_id<<"into the memory"<<endl;
         // initalize page information
           page_id_dir++;
           Page_Dir->setNextPage(page_id_dir);
           Page_Dir->init(page_id_dir);
           Page_Dir->setPrevPage(page_id_dir-1);
         //  cout<<"Insert directory pageID = "<<page_id_dir<<endl;
           P_insert=Page_Dir->insertRecord((char *)dpinfop,sizeof(DataPageInfo),Rid_dir);   // insert datapage information
           if(P_insert!=OK)
            cout<<"Error: insert first directory record   pageid= "<<page_id_dir<<endl;
            P_page=MINIBASE_DB->write_page(page_id_dir,P_pin);
              if(P_page!=OK)
              cout<<"Error: cann not write page "<<page_id_dir<<"into the memory"<<endl;
            allocDataPageRid=Rid_dir;
          }
        else
        {
         //     Page *P_pin=new Page();
              memcpy(P_pin,Page_Dir,sizeof(HFPage)); 
              P_page=MINIBASE_DB->write_page(page_id_dir,P_pin);   // write data back
              if(P_page!=OK)
              cout<<"Error: cann not write page "<<page_id<<"into the memory"<<endl;
             allocDataPageRid=Rid_dir;
        }
   }

     delete P_pin;
    // fill in the body
    return OK;
}

// *******************************************
