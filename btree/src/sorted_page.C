/*
 * sorted_page.C - implementation of class SortedPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "sorted_page.h"
#include "btindex_page.h"
#include "btleaf_page.h"
const char* SortedPage::Errors[SortedPage::NR_ERRORS] = {
  //OK,
  //Insert Record Failed (SortedPage::insertRecord),
  //Delete Record Failed (SortedPage::deleteRecord,
};


/*
 *  Status SortedPage::insertRecord(AttrType key_type, 
 *                                  char *recPtr,
 *                                    int recLen, RID& rid)
 *
 * Performs a sorted insertion of a record on an record page. The records are
 * sorted in increasing key order.
 * Only the  slot  directory is  rearranged.  The  data records remain in
 * the same positions on the  page.
 *  Parameters:
 *    o key_type - the type of the key.
 *    o recPtr points to the actual record that will be placed on the page
 *            (So, recPtr is the combination of the key and the other data
 *       value(s)).
 *    o recLen is the length of the record to be inserted.
 *    o rid is the record id of the record inserted.
 */

Status SortedPage::insertRecord (AttrType key_type,
                                 char * recPtr,
                                 int recLen,
                                 RID& rid)
{
      // insert record to a page
    Status  Sort_insert=HFPage::insertRecord(recPtr,recLen,rid);
    if(Sort_insert!=OK) return DONE;    // full 
  //  if(HFPage::available_space()<16) return DONE;
    int high=HFPage::slotCnt-2;
    if (high<0) return OK;
    RID first,rid_high;
    HFPage::firstRecord(first);
    int low=first.slotNo;
    int media=(low+high)/2-1;  // move forward one slot to avoid un-used slot
    RID curRId,nextRid;
    curRId.pageNo=HFPage::curPage;
    curRId.slotNo=media;
     Status  Med=HFPage::nextRecord (curRId,nextRid);
    if(Med!=OK) media=0;
    else
    media=nextRid.slotNo;
    slot_t  last=HFPage::slot[rid.slotNo];  // find position for newest insert slot
    int entry_len,rec_Len;
    void *key1,*key2;
    if(key_type==attrInteger)
    {
        entry_len=4;
        Key_Int *a=(Key_Int *) recPtr;
        key1=(void *)(&a->intkey);
    }
    else  if(key_type==attrString)
    {
       entry_len=8;
        Key_string *a=(Key_string *) recPtr;
      //  key1=(void *)a->charkey.c_str();
         key1=(void *)a->charkey;
    }
    // scan whole , find a place to insert 
    char *recPtr_comp;
    int i=low;
     while(i<=high)
    {
        nextRid.slotNo=i;
        HFPage::returnRecord(nextRid,recPtr_comp,rec_Len);
        if(key_type==attrInteger)
        {
            entry_len=4;
            Key_Int *a=(Key_Int *) recPtr_comp;
            key2=(void *)(&a->intkey);
        }
        else  if(key_type==attrString)
        {
            entry_len=8;
            Key_string *a=(Key_string *) recPtr_comp;
          //  key2=(void *)a->charkey.c_str();
             key2=(void *)a->charkey;
        }
          if(keyCompare(key1, key2, key_type)<0)
         {
          // find the place , and insert.
            for(int j=HFPage::slotCnt-2;j>i-1;j--)
            {
              //all slot position after j move back 1 
                HFPage::slot[j+1]=HFPage::slot[j];
            }
            // insert
              HFPage::slot[i]=last;
              rid.slotNo=i;
            break;
        }
        else
        {
             curRId.slotNo=i;
             HFPage::nextRecord (curRId,nextRid);
             i=nextRid.slotNo;
        }
    }

    
    // put your code here
    return OK;
}


/*
 * Status SortedPage::deleteRecord (const RID& rid)
 *
 * Deletes a record from a sorted record page. It just calls
 * HFPage::deleteRecord().
 */

char  big_key[220];

Status SortedPage::deleteRecord (const RID& rid)
{
       return HFPage::deleteRecord(rid);
  // put your code here
   // return OK;
}

int SortedPage::numberOfRecords()
{
  // put your code here
  return 0;
}


Status  SortedPage::Big_key(AttrType key_type,void *key,int &keylen)
{
          // return biggest key of a page, normally , the biggest key slot lie in the end of slot arry

           int i,reclen;
           RID Big_rid;
           char *recPtr_comp;
           for(i=HFPage::slotCnt-1;i>=0;i--)
           {
                if(HFPage::slot[i].length>0)
                  break;
           }
           Big_rid.pageNo=HFPage::curPage;
           Big_rid.slotNo=i;
          Status leav_insert=HFPage::returnRecord(Big_rid,recPtr_comp,reclen);
     
           if(leav_insert!=OK) 
            cout<<"Error: can not get bigger key record from page "<<HFPage::curPage<<endl;
            if(key_type==attrInteger)
           {
            
            Key_Int *a=(Key_Int *) recPtr_comp;
            memcpy(key,&a->intkey,sizeof(int));
            keylen=sizeof(int);
        
          }
          else  if(key_type==attrString)
          {

             Key_string *a=(Key_string *) recPtr_comp;
              memcpy(key,a->charkey,sizeof(a->charkey));
               keylen=sizeof(a->charkey);
          }
          return OK;
}
