/*
 * key.C - implementation of <key,data> abstraction for BT*Page and 
 *         BTreeFile code.
 *
 * Gideon Glass & Johannes Gehrke  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include <string.h>
#include <assert.h>

#include "bt.h"

/*
 * See bt.h for more comments on the functions defined below.
 */

/*
 * Reminder: keyCompare compares two keys, key1 and key2
 * Return values:
 *   - key1  < key2 : negative
 *   - key1 == key2 : 0
 *   - key1  > key2 : positive
 */
int keyCompare(const void *key1, const void *key2, AttrType t)
{
    
            if(t==attrInteger) 
            { 
              int *c=(int *) key1; int *d=(int *)key2; 
                        if(*c>*d) return 1;
                        else if(*c<*d) 
                          return -1;
                        else return 0;
                      //  break;
            }
            else
              if(t==attrString)
                  {
                        char *a=(char *)key1; char *b=(char *)key2;
                        if(strcmp(a,b)>0) return 1;
                        else if(strcmp(a,b)<0) return -1;
                        else return 0;
                      //  break;
                  }
              else
        
               cout<<"Error: AttrType dose not belong int or string"<<endl; 
     //   break; 
  
  // put your code here
  return -100;
}

/*
 * make_entry: write a <key,data> pair to a blob of memory (*target) big
 * enough to hold it.  Return length of data in the blob via *pentry_len.
 *
 * Ensures that <data> part begins at an offset which is an even 
 * multiple of sizeof(PageNo) for alignment purposes.
 */
void make_entry(KeyDataEntry *target,
                AttrType key_type, const void *key,
                nodetype ndtype, Datatype data,
                int *pentry_len)
{
      if(key_type==attrInteger)
    {
        int *a=(int *) key;
        (*target).key.intkey=*a;
        if(ndtype==INDEX)
        {
            
            (*target).data.pageNo=data.pageNo;
            *pentry_len=sizeof(int)+sizeof(PageId);
        }
        else
        {
            (*target).data.rid=data.rid;
            *pentry_len=sizeof(int)+sizeof(RID);
        }
    }
    else if(key_type==attrString)
    {
      
        char *d=(char *)key;
        strcpy((*target).key.charkey,d);
        if(ndtype==INDEX)
        {
            (*target).data.pageNo=data.pageNo;
            *pentry_len=sizeof(key)+sizeof(PageId);
        }
        else
        {
            (*target).data.rid=data.rid;
            *pentry_len=sizeof(key)+sizeof(RID);
        }
        
    }
    else
        std::cout<<"Wrong key type "<<endl;
  // put your code here
  return;
}


/*
 * get_key_data: unpack a <key,data> pair into pointers to respective parts.
 * Needs a) memory chunk holding the pair (*psource) and, b) the length
 * of the data chunk (to calculate data start of the <data> part).
 */
void* get_key_data(void *targetkey, Datatype *targetdata,
                  KeyDataEntry *psource, int entry_len, nodetype ndtype)
{
           
      if(ndtype==INDEX)
    {
       
        targetdata->pageNo=(*psource).data.pageNo;
        
    }
    else
    {
        (*targetdata).rid=(*psource).data.rid;
    }
    
    
    if(entry_len==4)
    {
     //    *all=((*psource).key.intkey);
        return &((*psource).key.intkey);
        
    }
    else
    {
        targetkey=&((*psource).key.charkey);
        return &((*psource).key.charkey);
    }
    
    return NULL;
   // put your code here
         // return;
}

/*
 * get_key_length: return key length in given key_type
 */
int get_key_length(const void *key, const AttrType key_type)
{  
   if(key_type==attrInteger)
        return sizeof(int);
    else
        if(key_type==attrString)
        {
            char *a=(char *)key;
              string len=a;
            return  len.size();
        }
            // put your code here
            return 0;
 // put your code here
 return 0;
}
 
/*
 * get_key_data_length: return (key+data) length in given key_type
 */   
int get_key_data_length(const void *key, const AttrType key_type, 
                        const nodetype ndtype)
{

     if(key_type==attrInteger)
    {
        if(ndtype==INDEX)
            return sizeof(int)+sizeof(PageId);
        else
            return sizeof(int)+sizeof(RID);
    } else
        if(key_type==attrString)
        {
              char *a=(char *)key;
              string len=a;
          //    cout<<len.c_str()<<" key data length "<<len.size()<<endl;
            if(ndtype==INDEX)
                return len.size()+sizeof(PageId);
            else
                return len.size()+sizeof(RID);
        }
 // put your code here
 return 0;
}
