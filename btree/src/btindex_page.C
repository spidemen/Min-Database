/*
 * btindex_page.C - implementation of class BTIndexPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "btindex_page.h"
#include "db.h"
// Define your Error Messge here
const char* BTIndexErrorMsgs[] = {
  //Possbile error messages,
  //OK,
  //Record Insertion Failure,
};

static error_string_table btree_table(BTINDEXPAGE, BTIndexErrorMsgs);

Status BTIndexPage::insertKey (const void *key,
                               AttrType key_type,
                               PageId pageNo,
                               RID& rid)
{

     int recLen,pentry_len;
     char *recPtr;
    KeyDataEntry target;
    Datatype data;
     Status Index_insert;
    data.pageNo=pageNo;
    // make a entry
     make_entry(&target,key_type,key,INDEX,data,&pentry_len);

     if(key_type==attrInteger)
     {
         Key_Int a;
         a.intkey=target.key.intkey;
         a.data.pageNo=pageNo;
        recPtr=(char*)(&a);
        // get key data length and insert
       recLen=get_key_data_length(key,key_type,INDEX);
      Index_insert=SortedPage::insertRecord ( key_type,recPtr,recLen,rid);
      
     }
    else if(key_type==attrString)
    {
        Key_string a;
       memcpy(a.charkey,target.key.charkey,sizeof(a.charkey));
        a.data.pageNo=target.data.pageNo;
         recPtr=(char*)(&a);
         // get key data length and return
            recLen=sizeof(a);
        Index_insert=SortedPage::insertRecord ( key_type,recPtr,recLen,rid);
  
    }
  
 
     if(Index_insert!=OK)  
      {   // cout<<"test index page full "<<endl; 
      return FILEEOF;  } // page full
     if(HFPage::available_space()<350)
      {  // cout<<"test index page half full "<<endl; 
       return DONE;   } // 50% percentage full
 
    return OK;
}

Status BTIndexPage::deleteKey (const void *key, AttrType key_type, RID& curRid)
{

      
      int pageNo;
      RID rid;
      void *key1=new char[220];
      get_first(rid,key1,pageNo);
    
      if(keyCompare(key,key1, key_type)==0)
      {
        if(SortedPage::deleteRecord(rid)!=OK)

          cout<<"Fail delete  first...."<<endl;
       
      }
      else
      {
        while(get_next(rid,key1,pageNo)==OK)
        {
          if(keyCompare(key,key1,key_type)==0)
          {
       
            if(SortedPage::deleteRecord(rid)!=OK)
              cout<<"Fail delete  next...."<<endl;
           
             break;
          }
        }

      }
  // put your code here
  return OK;
}

Status BTIndexPage::get_page_no(const void *key,
                                AttrType key_type,
                                PageId & pageNo)
{

  // find key , scan from first to the last
        RID first,next;
        Status get_rid;
        get_rid=SortedPage::firstRecord(first);
        if(get_rid!=OK)   
        {
        //  cout<<"Error: get first rid of HFpage "<<endl; 
          return DONE;
        }
        char *recPtr_comp;
        void *key2;
        int rec_Len,flag_find=0,flag_first=1;
        int flag_test=0;
        while(SortedPage::nextRecord(first,next)==OK)
        {
           HFPage::returnRecord(first,recPtr_comp,rec_Len);
           if(key_type==attrInteger)
           {
      
            Key_Int *a=(Key_Int *) recPtr_comp;
     
            key2=(void *)(&a->intkey);
             if(keyCompare(key, key2, key_type)<=0)
              {
                // most range scan, find closest key
                 
                  pageNo=a->data.pageNo;
                  flag_find=1;
                  break;
                 
                
              }
          }
          else  if(key_type==attrString)
          {
    
            Key_string *a=(Key_string *) recPtr_comp;
            key2=(void *)(a->charkey);

             if(keyCompare(key, key2, key_type)<=0)
              {
                 // most range scan, find closest key

                 // tow or more level, find index page
                  if(this->level>1)
                  {
                      Page *leaf_write=new Page();
                     Status  index_insert=MINIBASE_DB->read_page(a->data.pageNo,leaf_write);
                     // go to index page , find leaf page number
                      BTIndexPage *index1=(BTIndexPage *)leaf_write;
                      int page_no;
                      index1->get_page_no(key,key_type,page_no);
                      pageNo=page_no;
                       flag_find=1;
                       delete leaf_write;
                  }
                  else
                  {
                    // just one level, return page numebr
                    pageNo=a->data.pageNo;
                    flag_find=1;

              
                   break;
                 }
              }
          } 
          first=next;
          flag_test++;
      }

// do it again for final element in the page
       if(!flag_find)
        {
          HFPage::returnRecord(first,recPtr_comp,rec_Len);
          if(key_type==attrInteger)
           {
            
            Key_Int *a=(Key_Int *) recPtr_comp;
            key2=(void *)(&a->intkey);
             if(keyCompare(key, key2, key_type)<=0)
              {
            
                  pageNo=a->data.pageNo;
                   flag_find=1;
            
              }
              else
              {
                  pageNo=a->data.pageNo;
             
              }
          }
          else  if(key_type==attrString)
          {
            Key_string *a=(Key_string *) recPtr_comp;
            key2=(void *)a->charkey;

             if(keyCompare(key, key2, key_type)<=0)
              {

                 // tow or more level, find index page
                if(this->level>1)
                  {
                      Page *leaf_write=new Page();
                     Status  index_insert=MINIBASE_DB->read_page(a->data.pageNo,leaf_write);
                      BTIndexPage *index1=(BTIndexPage *)leaf_write;
                      // go to index page , find leaf page number
                      int page_no;
                      index1->get_page_no(key,key_type,page_no);
                      pageNo=page_no;
                       flag_find=1;
                       delete leaf_write;
                  }
                  else
                  {
                  pageNo=a->data.pageNo;
                    flag_find=1;
                  }
                 
              }
              else
                  pageNo=a->data.pageNo;
          }
      }
       if(!flag_find) return FAIL;
  // put your code here
     return OK;
}

Status BTIndexPage::get_index_Page_no(const void *key,
                                AttrType key_type,
                                PageId & pageNo)
{
        RID first,next;
        Status get_rid;
        get_rid=SortedPage::firstRecord(first);
        if(get_rid!=OK)   
        {
        //  cout<<"Error: get first rid of HFpage "<<endl; 
          return DONE;
        }
        char *recPtr_comp;
        void *key2;
        int rec_Len,flag_find=0,flag_first=1;
       while(SortedPage::nextRecord(first,next)==OK)
       {
          HFPage::returnRecord(first,recPtr_comp,rec_Len);
          if(key_type==attrString)
          {
    
            Key_string *a=(Key_string *) recPtr_comp;
            key2=(void *)(a->charkey);

             if(keyCompare(key, key2, key_type)<=0)
              {
                 // most range scan, find closest key
                    pageNo=a->data.pageNo;
                    flag_find=1;
                   break;
                 
              }
          } 

           first=next;

       }

    if(!flag_find)
    {
         HFPage::returnRecord(first,recPtr_comp,rec_Len);
        if(key_type==attrString)
          {
             Key_string *a=(Key_string *) recPtr_comp;
            key2=(void *)(a->charkey);
             if(keyCompare(key, key2, key_type)<=0)
              {
                 // most range scan, find closest key
                    pageNo=a->data.pageNo;
                    flag_find=1;
                 
              }
              else
                 pageNo=a->data.pageNo;
          }
    }


   if(!flag_find) return FAIL;
  // put your code here
     return OK;

}

    
Status BTIndexPage::get_first(RID& rid,
                              void *key,
                              PageId & pageNo)
{

  // get first element in the index page and rerturn leaf page number
   //   RID  first;
      char *recPtr_comp;
      int rec_Len;
       if(SortedPage::firstRecord(first_index)!=OK) cout<<"can not find first record in the first index page "<<endl;
       if(HFPage::returnRecord(first_index,recPtr_comp,rec_Len)!=OK)  cout<<"can not get record from the first index page "<<endl;
       if(this->keytype==attrInteger)
           {
                  Key_Int *a=(Key_Int *) recPtr_comp;
                   pageNo=a->data.pageNo;
                    memcpy(key,&(a->intkey),sizeof(int));
              
               
          }
          else  if(this->keytype==attrString)
          {
                 Key_string *a=(Key_string *) recPtr_comp;       
                  pageNo=a->data.pageNo;
                memcpy(key,&a->charkey,sizeof(a->charkey));
            
          }
          rid=first_index;


  // put your code here
  return OK;
}

Status BTIndexPage::get_next(RID& rid, void *key, PageId & pageNo)
{
       
       // get next record of the index page, return key values and page number
      RID next;
      char *recPtr_comp;
      int rec_Len;
        if(HFPage::nextRecord(first_index,next)!=OK)
          return DONE;
        else
        {
            first_index=next;
           if(HFPage::returnRecord(first_index,recPtr_comp,rec_Len)!=OK)  cout<<"can not get record from the first index page "<<endl;
            if(this->keytype==attrInteger)
           {
            
                  Key_Int *a=(Key_Int *) recPtr_comp;
                  pageNo=a->data.pageNo;
                    memcpy(key,&(a->intkey),sizeof(int));
           
               
          }
          else  if(this->keytype==attrString)
          {
                 Key_string *a=(Key_string *) recPtr_comp;       
                  pageNo=a->data.pageNo;
                 memcpy(key,&a->charkey,sizeof(a->charkey));
            
          }
        }
        rid=first_index;

  // put your code here
  return OK;
}

