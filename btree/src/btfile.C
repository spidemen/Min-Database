/*
 * btfile.C - function members of class BTreeFile 
 * 
 * Johannes Gehrke & Gideon Glass  951022  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

// Define your error message here
const char* BtreeErrorMsgs[] = {
  // Possible error messages
  // _OK
  // CANT_FIND_HEADER
  // CANT_PIN_HEADER,
  // CANT_ALLOC_HEADER
  // CANT_ADD_FILE_ENTRY
  // CANT_UNPIN_HEADER
  // CANT_PIN_PAGE
  // CANT_UNPIN_PAGE
  // INVALID_SCAN
  // SORTED_PAGE_DELETE_CURRENT_FAILED
  // CANT_DELETE_FILE_ENTRY
  // CANT_FREE_PAGE,
  // CANT_DELETE_SUBTREE,
  // KEY_TOO_LONG
  // INSERT_FAILED
  // COULD_NOT_CREATE_ROOT
  // DELETE_DATAENTRY_FAILED
  // DATA_ENTRY_NOT_FOUND
  // CANT_GET_PAGE_NO
  // CANT_ALLOCATE_NEW_PAGE
  // CANT_SPLIT_LEAF_PAGE
  // CANT_SPLIT_INDEX_PAGE
};


#define MAX_Page 500
BTLeafPage *leaf=new BTLeafPage();
BTIndexPage *index1=new BTIndexPage();
BTIndexPage *root=new BTIndexPage();
int flag_tree_insert=0;
static error_string_table btree_table( BTREE, BtreeErrorMsgs);

BTreeFile::BTreeFile (Status& returnStatus, const char *filename)
{
      int head_start;
     if(MINIBASE_DB->get_file_entry(filename,head_start)==OK)
        {
            // get tree infor including key type
                Page *head_pag;
               this->header=head_start;
                Status head_pin=MINIBASE_BM->pinPage(head_start,head_pag,1);
              if(head_pin!=OK)
                { cout<<"Error: can not pin the head page  "<<head_pag<<endl;
                 returnStatus=FAIL;
                }
               HeadPage *head=new HeadPage();
               head=(HeadPage *)head_pag;
              this->keytype=head->keytype;
          //     cout<<"enter into Btree file "<<endl;
        }
        else
          cout<<"Fata ERROR: can not find the file entry in the DB"<<endl;
        returnStatus=OK;
  // put your code here
}

BTreeFile::BTreeFile (Status& returnStatus, const char *filename, 
                      const AttrType keytype,
                      const int keysize)
{
        int start_pg,head_start;
      if(MINIBASE_DB->get_file_entry(filename,head_start)==OK)
        {
          // get tree infor including key type
               this->header=head_start;
               this->index_number=0;
               this->keytype=keytype;
        }
      else
      {
        Status Root=MINIBASE_DB->allocate_page(start_pg);   // allocate a page for root 
        Status Head=MINIBASE_DB->allocate_page(head_start); // allocate a page for head
        if(Head==OK)
        Head=MINIBASE_DB->add_file_entry(filename,head_start);   // add entry 
       if(Head!=OK)
        cout<<"Error: Cannot add file entry "<<filename<<endl;
       Page *head_pag;
       Status head_pin=MINIBASE_BM->pinPage(head_start,head_pag,1);   // pin head page 
       if(head_pin!=OK)
       { 
        cout<<"Error: can not pin the head page  "<<head_pag<<endl;
          returnStatus=FAIL;
       }
       // init head page 
       HeadPage *head=new HeadPage();
       head->root=start_pg;
       head->keytype=keytype;
       head->keyLength=keysize;
       head->index_page_number=0;
       head->filename=filename;
       //  store key B+ tree data type
       this->header=head_start;
       this->index_number=0;
       this->keytype=keytype;
       memcpy(head_pag,head,sizeof(HeadPage));
       flag_tree_insert=0;
    //  strcpy((char *)head_pag,(char *)head);
       root->init(start_pg);  // init root page 
       root->level=1;
    }

    returnStatus=OK;


  // put your code here
}

BTreeFile::~BTreeFile ()
{


  // put your code here
}

Status BTreeFile::destroyFile ()
{
  
  Status destory_page;
  RID rid;
  // init key
  void *key;
   if(this->keytype==attrInteger)  
      {
        int a;
        key=(void *)&a;
      }
      else
      {
        string a;
        key=(void *)&a;
      }
  PageId pageNo;
  Page *head_pag;
 
  destory_page=root->get_first(rid,key,pageNo); // get first  leaf page number
  destory_page=MINIBASE_DB->deallocate_page(pageNo);   // decollate first leaf page
  while(root->get_next(rid,key,pageNo)==OK)       // dellocate other leaf page
  {
      destory_page=MINIBASE_DB->deallocate_page(pageNo);
  
  }

   destory_page=MINIBASE_DB->deallocate_page(5);
   int head_start=this->header;
   Status head_pin=MINIBASE_BM->pinPage(head_start,head_pag,1);
    HeadPage *head=new HeadPage();
    head=(HeadPage *)head_pag;
    destory_page=MINIBASE_DB->deallocate_page(head->root);  // dellocate root page
 
   Status Head=MINIBASE_DB->delete_file_entry(head->filename.c_str());  // delete B + entry

    MINIBASE_BM->freePage(head_start);      // free a pin head page

  
    // put your code here
  return OK;
}

Status BTreeFile::insert(const void *key, const RID rid) {
  
    // get tree infro  from head page
      Page *head_pag;
      HeadPage *head=new HeadPage();
      Status head_pin=MINIBASE_BM->pinPage(this->header,head_pag,1);
       if(head_pin!=OK)
       { cout<<"Error: can not pin the head page  "<<this->header<<endl;
       //   returnStatus=FAIL;
       }
      head=(HeadPage *)head_pag;
      AttrType key_type=this->keytype;
      RID leav_rid,index_rid,root_rid;
      Status leav_insert,index_insert,alloc_page,root_insert, page_write,leaf_pin;
      int  leaf_alloc,index_alloc,reclen;
      Page *leaf_write=new Page();
      char *recPtr_comp;
      RID Big_rid,dataRid;
      void *key2=new char[220];
      // first insert 
      if(!flag_tree_insert)
      {
         alloc_page=MINIBASE_DB->allocate_page(leaf_alloc);
         if(alloc_page!=OK) cout<<"Error: can not allocate a page for  leavf "<<endl;
         leaf->init(leaf_alloc);
         flag_tree_insert=1;
      }
      int pageNo;
  
      // check key whether in the leaf node, guarteen key unique
      root_insert=root->get_page_no(key,this->keytype,pageNo);
      if(root_insert==OK)
      {
       Page *leaf_read=new Page();
    
       page_write=MINIBASE_DB->read_page(pageNo,leaf_read);
      if(page_write!=OK)  cout<<"Cann not read the leaf page  check key "<<pageNo<<endl;
       BTLeafPage *leaf1=(BTLeafPage *)leaf_read;
       if(page_write==OK&&leaf1->get_data_rid(key,this->keytype,dataRid)==OK)  // find key, abadon duplicate
        return OK;

       delete leaf_read;
     }


     if(root_insert!=OK)
     {
      // if root insert is OK, mean it can find a leaf page, otherwise can no, which need allocate a new page 
      leav_insert=leaf->insertRec(key,key_type,rid,leav_rid);
      // page 50%  full split 
      if(leav_insert!=OK&&leav_insert==DONE)
      {
           int keylen;

           leaf->Big_key(key_type,key2,keylen);

           char temp[keylen];
           memcpy(temp,key2,keylen);
           const void *key3=temp;
           // write the biggest key and page number to the index page
         if(root->level==1)         // one level just do insert
          root_insert=root->insertKey(key3,key_type,leav_rid.pageNo,root_rid);
        else
         {  
          // 2 two level , first do searh in index pages 
            int index_page_no;
            RID rid1;
             void *key_insert=new char[220];  
            if(root->get_index_Page_no(key3,this->keytype,index_page_no)==OK)  // get index page number
            {
               
                 index_insert=MINIBASE_DB->read_page(index_page_no,leaf_write);
                 BTIndexPage *index3=(BTIndexPage *)leaf_write;
                 root_insert=index3->insertKey(key3,key_type,leav_rid.pageNo,root_rid);     // find index page number and insert
                 index3->Big_key(key_type,key_insert,keylen);    // get the biggest key of index page
                 if(keyCompare(key3, key_insert, key_type)<=0)    // key bigger than root node key, update key in root node
                 {
                      root->deleteKey(key3,key_type,rid1);        
                      root_insert=root->insertKey(key_insert,key_type,index_page_no,leav_rid);
                 }

                  leaf_write=(Page *)index3;
                  MINIBASE_DB->write_page(index_page_no,leaf_write);
              
            }
            else
            {
              // no find key in root node, mean it bigger than anyone key in root node, go to index page, update key in the root node
                 index_insert=MINIBASE_DB->read_page(index_page_no,leaf_write);    // go to right index page
                 BTIndexPage *index3=(BTIndexPage *)leaf_write;
                 root_insert=index3->insertKey(key3,key_type,leav_rid.pageNo,root_rid);     // insert key 
                 index3->Big_key(key_type,key_insert,keylen);    // get the biggest key of index page
                 //update key in the root node (more bigger than before)
                  root->Big_key(key_type,key_insert,keylen); 
                  root->deleteKey(key_insert,key_type,rid1);
                  root_insert=root->insertKey(key3,key_type,index_page_no,leav_rid);
                  leaf_write=(Page *)index3;
                  MINIBASE_DB->write_page(index_page_no,leaf_write);   // update index page
              
            }  

         }
          if(root_insert!=OK&&root_insert==FILEEOF)
          {        
#if 1
            // index level +1, split root node into 2 index page 
             root->level++;
            int insert_prob=0;
            RID rid1;
            void *key_insert=new char[220];       
            //allocate a new page 
             alloc_page=MINIBASE_DB->allocate_page(leaf_alloc);
             if(alloc_page!=OK) cout<<"Error: can not allocate a page for  index "<<endl;
              index_insert=MINIBASE_DB->read_page(leaf_alloc,leaf_write);
              BTIndexPage *index1=(BTIndexPage *)leaf_write;
               index1->init(leaf_alloc);
              // split root page 
               root->keytype=key_type;
               root->get_first(rid1,key_insert,pageNo);
               index_insert=index1->insertKey(key_insert,key_type,pageNo,leav_rid);
               root->deleteKey(key_insert,key_type,rid1);
               while(index_insert!=DONE)          // put formal half of root record into left index page
               {
               
                 root->get_next(rid1,key_insert,pageNo);
                 index_insert=index1->insertKey(key_insert,key_type,pageNo,leav_rid);
                  root->deleteKey(key_insert,key_type,rid1);
                   if(!insert_prob&&root->insertKey(key3,key_type,pageNo,leav_rid)!=DONE)
                   {
                    insert_prob=1;
                   }

               }

             index1->Big_key(key_type,key_insert,keylen);    // get the biggest key of page
             index1->level=1;

           char temp[keylen];
           memcpy(temp,key_insert,keylen);
           const void *key3=temp;
              // write index page into disk
            leaf_write=(Page *)index1;
            page_write=MINIBASE_DB->write_page(leaf_alloc,leaf_write);
           if(page_write!=OK) cout<<"Error: can not write a root page to the disk  page number "<<leav_rid.pageNo<<endl;
            // write index page number into root page 
              dataRid.pageNo=leaf_alloc;
               int leaf_alloc2;
               alloc_page=MINIBASE_DB->allocate_page(leaf_alloc2);
             if(alloc_page!=OK) cout<<"Error: can not allocate a page for  index "<<endl;
           
              index_insert=MINIBASE_DB->read_page(leaf_alloc2,leaf_write);
              BTIndexPage *index2=(BTIndexPage *)leaf_write;
              index2->init(leaf_alloc);
               Status end_root=OK;
               index_insert=OK;
                end_root=root->get_next(rid1,key_insert,pageNo);
               while(index_insert!=DONE&&end_root!=DONE)      // put remain half root record into right index page
               {
                      
                 index_insert=index2->insertKey(key_insert,key_type,pageNo,leav_rid);
                  root->deleteKey(key_insert,key_type,rid1);
                   end_root=root->get_next(rid1,key_insert,pageNo);
               }
             index2->Big_key(key_type,key_insert,keylen);    // get the biggest key of page
             index2->level=1;
             leaf_write=(Page *)index2;
            page_write=MINIBASE_DB->write_page(leaf_alloc2,leaf_write);
           if(page_write!=OK) cout<<"Error: can not write a root page to the disk  page number "<<leav_rid.pageNo<<endl;
            // write index page number into root page 
              dataRid.pageNo=leaf_alloc2;
             leav_insert=root->insertKey(key_insert,key_type,leaf_alloc2,leav_rid);     // insert record of left index page with biggest key  into root node
              leav_insert=root->insertKey(key3,key_type,leaf_alloc,leav_rid);         // insert record of  right page with biggest key into root node
#endif
            
          }
      
          Page *leaf_write1=new Page();
          int leaf_page1=leaf->page_no();
          // allocate a new page for next time insert
           alloc_page=MINIBASE_DB->allocate_page(leaf_alloc);
           if(alloc_page!=OK) cout<<"Error: can not allocate a page for  leavf "<<endl;
           leaf->setPrevPage(leaf_alloc); // building page point
            memcpy(leaf_write1,leaf,sizeof(Page));
          page_write=MINIBASE_DB->write_page(leaf_page1,leaf_write1);
         if(page_write!=OK) cout<<"Error: can not write a leaf page to the disk  page number "<<leav_rid.pageNo<<endl;

         // init a new page , build page point 
            leaf->init(leaf_alloc); 
            leaf->setNextPage(leaf_page1);
      
          // write to disk
         memcpy(leaf_write1,leaf,sizeof(Page));
         page_write=MINIBASE_DB->write_page(leaf_alloc,leaf_write1);
         if(page_write!=OK) cout<<"Erro: can not write a leaf page to the disk  page number "<<leav_rid.pageNo<<endl;
         delete  leaf_write1;

      }
      else
      {
        
          memcpy(leaf_write,leaf,sizeof(Page));
          page_write=MINIBASE_DB->write_page(leav_rid.pageNo,leaf_write);
         if(page_write!=OK) cout<<"Error: can not write a leaf page to the disk  page number "<<leav_rid.pageNo<<endl;

      }
    }
   else
  {
     
       // find a lead page, then read leaf page to memory, inserting record
        Status leaf_Read=MINIBASE_DB->read_page(pageNo,leaf_write);
        if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<pageNo<<endl;
         BTLeafPage *leaf1=(BTLeafPage *)leaf_write;
         leav_insert=leaf1->insertRec(key,key_type,rid,leav_rid);
       
         if(leav_insert==FILEEOF)
         {
          //Page 100% Full, split

           Page *leaf_write2=new Page();
           Page *leaf_temp=new Page();
           int temp_page;
           // allocate a new lead page
            alloc_page=MINIBASE_DB->allocate_page(leaf_alloc);
           if(alloc_page!=OK) cout<<"Error: can not allocate a page for  leavf "<<endl;
            temp_page=leaf1->getNextPage();   // recording from page point
            if(temp_page!=-1)
            {
               // no next page
             leaf_Read=MINIBASE_DB->read_page(temp_page,leaf_temp);
            if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<temp_page<<endl;
            BTLeafPage *leaf_t=(BTLeafPage *)leaf_temp;
            leaf_t->setPrevPage(leaf_alloc);    // build a page point
            leaf_Read=MINIBASE_DB->write_page(temp_page,leaf_temp);
            if(leaf_Read!=OK) cout<<"Cann not write the leaf page  "<<temp_page<<endl;
            }
           leaf1->setNextPage(leaf_alloc);    // update page point
           leaf_Read=MINIBASE_DB->read_page(leaf_alloc,leaf_write2);
           if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<leaf_alloc<<endl;
            BTLeafPage *leaf2=(BTLeafPage *)leaf_write2;
            // init a new page and build  page point
            leaf2->init(leaf_alloc);
            leaf2->setPrevPage(pageNo);
            leaf2->setNextPage(temp_page);    // insert between two page
        
            if(key_type==attrInteger||key_type==attrString)
            {
                Key_Int a;
                Key_string b;
               char *stringkey=new char[220];
               RID dataRid,rid1;
               int insert_prob=0;
              void *key_insert;
               if(key_type==attrInteger)
               {
                key_insert=&a.intkey;
               }
               else
               {
                  key_insert=stringkey;
               }
              // split a page into two page
               leaf1->keytype=key_type;
               leaf1->get_first(rid1,key_insert,dataRid);
               leav_insert=leaf2->insertRec(key_insert,key_type,dataRid,leav_rid);
               leaf1->HFPage::deleteRecord(rid1);
               // 50 % FUll , insert 50 % to other page
               while(leav_insert!=DONE)
               {
               
                  leaf1->get_next(rid1,key_insert,dataRid);
                 leav_insert=leaf2->insertRec(key_insert,key_type,dataRid,leav_rid);
                 leaf1->HFPage::deleteRecord(rid1);
                   if(!insert_prob&&leaf1->insertRec(key,key_type,rid,leav_rid)!=FILEEOF)     // insert a record
                   {
                    insert_prob=1;
                   }

               }

          
           int keylen;
           leaf2->Big_key(key_type,key2,keylen);    // get the biggest key of page
           char temp[keylen];
           memcpy(temp,key2,keylen);
           const void *key3=temp;

           // insert this key to the index page

         if(root->level==1)
          root_insert=root->insertKey(key3,key_type,leav_rid.pageNo,root_rid);
        else
         {
            int index_page_no;
            RID rid1;
             void *key_insert=new char[220];  
            if(root->get_index_Page_no(key3,this->keytype,index_page_no)==OK)
            {
               
                 index_insert=MINIBASE_DB->read_page(index_page_no,leaf_write);
                 BTIndexPage *index3=(BTIndexPage *)leaf_write;
                 root_insert=index3->insertKey(key3,key_type,leav_rid.pageNo,root_rid);
                 index3->Big_key(key_type,key_insert,keylen);    // get the biggest key of page
                 if(keyCompare(key3, key_insert, key_type)<=0)
                 {
                      root->deleteKey(key3,key_type,rid1);
                      root_insert=root->insertKey(key_insert,key_type,index_page_no,leav_rid);
                 }

                  leaf_write=(Page *)index3;
                  MINIBASE_DB->write_page(index_page_no,leaf_write);
              
            }
            else
            {
                 index_insert=MINIBASE_DB->read_page(index_page_no,leaf_write);
                 BTIndexPage *index3=(BTIndexPage *)leaf_write;
                 root_insert=index3->insertKey(key3,key_type,leav_rid.pageNo,root_rid);
                 index3->Big_key(key_type,key_insert,keylen);    // get the biggest key of page
                  root->deleteKey(key_insert,key_type,rid1);
                  root_insert=root->insertKey(key3,key_type,index_page_no,leav_rid);
                  leaf_write=(Page *)index3;
                  MINIBASE_DB->write_page(index_page_no,leaf_write);
             
            }

         }

      if(root_insert!=OK&&root_insert==FILEEOF)  // in this project, just 2 level is enough, so the following spit root node do not excute 
       {

     
#if 1
          // index level +1, split root node into 2 index page 
             root->level++;
            int insert_prob=0;
            RID rid1;
            void *key_insert=new char[220];       
            //allocate a new page 
             alloc_page=MINIBASE_DB->allocate_page(leaf_alloc);
             if(alloc_page!=OK) cout<<"Error: can not allocate a page for  index "<<endl;
              index_insert=MINIBASE_DB->read_page(leaf_alloc,leaf_write);
              BTIndexPage *index1=(BTIndexPage *)leaf_write;
               index1->init(leaf_alloc);
              // split root page 
               root->keytype=key_type;
               root->get_first(rid1,key_insert,pageNo);
               index_insert=index1->insertKey(key_insert,key_type,pageNo,leav_rid);
               root->deleteKey(key_insert,key_type,rid1);
               while(index_insert!=DONE)          // put formal half of root record into left index page
               {
               
                 root->get_next(rid1,key_insert,pageNo);
                 index_insert=index1->insertKey(key_insert,key_type,pageNo,leav_rid);
                  root->deleteKey(key_insert,key_type,rid1);
                   if(!insert_prob&&root->insertKey(key3,key_type,pageNo,leav_rid)!=DONE)
                   {
                    insert_prob=1;
                   }

               }

             index1->Big_key(key_type,key_insert,keylen);    // get the biggest key of page
             index1->level=1;

           char temp[keylen];
           memcpy(temp,key2,keylen);
           const void *key3=temp;
              // write index page into disk
            leaf_write=(Page *)index1;
            page_write=MINIBASE_DB->write_page(leaf_alloc,leaf_write);
           if(page_write!=OK) cout<<"Error: can not write a root page to the disk  page number "<<leav_rid.pageNo<<endl;
            // write index page number into root page 
              dataRid.pageNo=leaf_alloc;


               int leaf_alloc2;
               alloc_page=MINIBASE_DB->allocate_page(leaf_alloc2);
             if(alloc_page!=OK) cout<<"Error: can not allocate a page for  index "<<endl;
              index_insert=MINIBASE_DB->read_page(leaf_alloc2,leaf_write);
              BTIndexPage *index2=(BTIndexPage *)leaf_write;
              index2->init(leaf_alloc);
               Status end_root=OK;
               index_insert=OK;
                end_root=root->get_next(rid1,key_insert,pageNo);
               while(index_insert!=DONE&&end_root!=DONE)      // put remain half root record into right index page
               {
                
                 index_insert=index2->insertKey(key_insert,key_type,pageNo,leav_rid);
                  root->deleteKey(key_insert,key_type,rid1);
                   end_root=root->get_next(rid1,key_insert,pageNo);
               }
             index2->Big_key(key_type,key_insert,keylen);    // get the biggest key of page
             index2->level=1;
             leaf_write=(Page *)index2;
            page_write=MINIBASE_DB->write_page(leaf_alloc2,leaf_write);
           if(page_write!=OK) cout<<"Error: can not write a root page to the disk  page number "<<leav_rid.pageNo<<endl;
            // write index page number into root page 
              dataRid.pageNo=leaf_alloc2;
              leav_insert=root->insertKey(key_insert,key_type,leaf_alloc2,leav_rid);     // insert record of left index page with biggest key  into root node
              leav_insert=root->insertKey(key3,key_type,leaf_alloc,leav_rid);           // insert record of  right page with biggest key into root node

         
#endif            
            }



           // write both leaf page to disk
             leaf_Read=MINIBASE_DB->write_page(pageNo,leaf_write);
              if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<pageNo<<endl;
              leaf_Read=MINIBASE_DB->write_page(leaf_alloc,leaf_write2);
              if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<leaf_alloc<<endl;

            }
           delete leaf_write2,leaf_temp;

         }
         else
         {
          // leaf page not 100% full , insert 
         leaf_Read=MINIBASE_DB->write_page(pageNo,leaf_write);
         if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<pageNo<<endl;
         }

         delete leaf_write;

    }

    
  // put your code here
  return OK;
}

Status BTreeFile::Delete(const void *key, const RID rid) {

        Status root_search, leaf_Read,leaf_search,leaf_pin;
        RID dataRid;
        PageId  pageNo;
        Page *leaf_read=new Page();
          // get leaf page number
        root_search=root->get_page_no(key,this->keytype,pageNo);
        if(root_search!=OK) {   
           leaf_Read=MINIBASE_DB->read_page(pageNo,leaf_read);
            if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<pageNo<<endl;
             BTLeafPage *leaf1=(BTLeafPage *)leaf_read;
             pageNo=leaf1->getPrevPage();
       
              }
        // read a leaf page , then search for the key
         leaf_pin=MINIBASE_BM->pinPage(pageNo,leaf_read,0,"Btree");
        leaf_Read=MINIBASE_DB->read_page(pageNo,leaf_read);
        if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<pageNo<<endl;
         BTLeafPage *leaf1=(BTLeafPage *)leaf_read;

         if(leaf1->get_data_rid(key,this->keytype,dataRid)==OK)
         {
          // find key , delete
             leaf1->HFPage::deleteRecord(dataRid);
             leaf_Read=MINIBASE_DB->write_page(pageNo,leaf_read);
            if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<pageNo<<endl;
         }
         else
         {

                   leaf_pin=MINIBASE_BM->unpinPage(pageNo,0,"Btree");
                     cout<<"can not find the record in the leaf page    page id "<<pageNo<<endl;
                      return FAIL;
              
         }

          leaf_pin=MINIBASE_BM->unpinPage(pageNo,0,"Btree");
  // put your code here
  return OK;
}
    
IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
  
      BTreeFileScan  *scan=new BTreeFileScan();
      IndexFileScan  *scan1=NULL;
      RID rid,dataRid;
      Status leaf_page,root_search,leaf_Read,root_search1;
      void *key;
      if(this->keytype==attrInteger)  
      {
        int a;
        key=(void *)&a;
      }
      else
      {
        key=new char[220];
      }
       Page *leaf_read=new Page();
      
      
      PageId  pageNo,PageNo_begin;
       root->keytype=this->keytype;
       scan->keytype=this->keytype;
      if(lo_key==NULL&&hi_key==NULL)
      {
       // whole scan  
        if(root->level==1)
          leaf_page=root->get_first(rid,key,pageNo);
        else
        {
            // two level should do search in the index page, find first page number
             int index_page_no;
            RID rid1;
             void *key_insert=new char[220];  
             root->get_first(rid,key,pageNo);
            if(root->get_index_Page_no(key,this->keytype,index_page_no)==OK)  // find index page number
            {
             
                 MINIBASE_DB->read_page(index_page_no,leaf_read);
                 BTIndexPage *index3=(BTIndexPage *)leaf_read;
                 index3->get_first(rid,key,pageNo);
             
            }

            else
              cout<<"Can not get leaf page number from index page "<<endl;
        }
          // get first leaf page
          scan->begin=pageNo;
          scan->end=MAX_Page;
          scan->R_Start.slotNo=-1;
          scan->R_End.slotNo=MAX_Page;
          scan1=scan;
        
          return scan;
      }
      else
        if(lo_key==NULL&&hi_key!=NULL)
        {
            // range scan , get higkey leaf page number and get first leaf page number
             if(root->level==1)
            leaf_page=root->get_first(rid,key,PageNo_begin);
             else
            {
              // two level should do search in the index page, find first page number
             int index_page_no;
            RID rid1;
             void *key_insert=new char[220];  
             root->get_first(rid,key,PageNo_begin);
              if(root->get_index_Page_no(key,this->keytype,index_page_no)==OK)  // find index page number
               { 
             
                 MINIBASE_DB->read_page(index_page_no,leaf_read);
                 BTIndexPage *index3=(BTIndexPage *)leaf_read;
                 index3->get_first(rid,key,PageNo_begin);
            
               }

              else
              cout<<"Can not get leaf page number from index page "<<endl;
           }
          

             root_search=root->get_page_no(hi_key,this->keytype,pageNo);
            if(root_search!=OK) { cout<<"can not find the key in the index page "<<endl;  }
             leaf_Read=MINIBASE_DB->read_page(pageNo,leaf_read);
            if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<pageNo<<endl;
            BTLeafPage *leaf1=(BTLeafPage *)leaf_read;
            root_search=leaf1->get_data_rid(hi_key,this->keytype,dataRid);
            if(root_search==OK||root_search==DONE)
            {
              // find key in the leaf page
               scan->begin=PageNo_begin;
               scan->end=pageNo;
                scan->R_Start.slotNo=-1;
               scan->R_End=dataRid;
               if(root_search==DONE) {dataRid.slotNo--;   scan->R_End=dataRid; }  // not find key, return close key rid
             
               return scan;
            }
              else
              {
              
                cout<<"Error: can not find  biggest search key "<<endl;
               scan->begin=PageNo_begin;
               scan->end=pageNo;
               cout<<"end page number "<<pageNo<<"  begin page "<<PageNo_begin<<endl;
              
              }

        }
        else if(lo_key!=NULL&&hi_key==NULL)
        {
             // range scan , get low leaf page number 
            root_search=root->get_page_no(lo_key,this->keytype,pageNo);
            if(root_search!=OK) { cout<<"can not find the key in the index page "<<endl;  }
             leaf_Read=MINIBASE_DB->read_page(pageNo,leaf_read);
            if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<pageNo<<endl;
            BTLeafPage *leaf1=(BTLeafPage *)leaf_read;
            root_search=leaf1->get_data_rid(lo_key,this->keytype,dataRid);
            if(root_search==OK||root_search==DONE)
            {
                // find key in the leaf page
               scan->begin=pageNo;
               scan->end=MAX_Page;
               scan->R_Start=dataRid;
               scan->R_End.slotNo=MAX_Page;
               return scan;
            }
              else
              {
              
                cout<<"Error: can not find  biggest search key "<<endl;
               scan->begin=PageNo_begin;
               scan->end=pageNo;
               cout<<"end page number "<<pageNo<<"  begin page "<<PageNo_begin<<endl;
              
              }
        }
        else 
        {

           Status root_search2;
            
            if(keyCompare(lo_key,hi_key,this->keytype)>0)  
            {
              // high key must bigger than low key
               scan->begin=-1;
               return scan;
            }
            PageId  low_key,higkey;
            RID  rid1,rid2;
            // find low and high key leaf page number
            root_search=root->get_page_no(lo_key,this->keytype,low_key);
            if(root_search==OK)
            {
            
            leaf_Read=MINIBASE_DB->read_page(low_key,leaf_read);
            if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<low_key<<endl;
            BTLeafPage *leaf1=(BTLeafPage *)leaf_read;
            // get key  rid
            root_search1=leaf1->get_data_rid(lo_key,this->keytype,rid1);
            }
            else root_search=FAIL;
            root_search=root->get_page_no(hi_key,this->keytype,higkey);
            if(root_search==OK) 
            {
            
             leaf_Read=MINIBASE_DB->read_page(higkey,leaf_read);
            if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<higkey<<endl;
            BTLeafPage *leaf2=(BTLeafPage *)leaf_read;
            // get key  rid
             root_search2=leaf2->get_data_rid(hi_key,this->keytype,rid2);
           }
           else root_search=FAIL;
            if((root_search==OK||root_search==DONE)&&(root_search2==OK||root_search2==DONE))
            {
              // find the key 
               scan->begin=low_key;
               scan->end=higkey;
               scan->R_Start=rid1;
               scan->R_End=rid2;
                 if(root_search1==DONE) {rid1.slotNo--;  scan->R_Start=rid1;  }  // not find key , return closest key
                if(root_search2==DONE) {rid2.slotNo--;  scan->R_End=rid2;  }    // not find key , return closest key
              if(root_search1==DONE&&root_search2==DONE&&rid1==rid2)  scan->begin=-1;
               return scan;
            }
            else if(root_search!=OK&&root_search!=DONE)
            {
              scan->begin=-1;
               return scan;
            }
            else
            {
               cout<<"Error: can not find  biggest search key "<<endl;
               scan->begin=PageNo_begin;
               scan->end=pageNo;
               cout<<"end page number "<<pageNo<<"  begin page "<<PageNo_begin<<endl;
              
            }

        }

        delete leaf_read;
        
   // put your code here
      
//    return NULL;
}

int keysize(){

// let it blank

  // put your code here
  return 0;
}
