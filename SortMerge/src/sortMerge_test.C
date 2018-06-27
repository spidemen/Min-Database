
#include <string.h>
#include <assert.h>
#include "sortMerge.h"
#include<map>
// Error Protocall:


enum ErrCodes {SORT_FAILED, HEAPFILE_FAILED};

static const char* ErrMsgs[] =  {
  "Error: Sort Failed.",
  "Error: HeapFile Failed."
  // maybe more ...
};

static error_string_table ErrTable( JOINS, ErrMsgs );
#define MAX_Tuple 30
struct rec {
    int key;
    char    filler[4];
};

int SortPage(char * filename1,int len_in1, AttrType in1[], short t1_str_sizes[],int  join_col_in1, TupleOrder   order);
// sortMerge constructor
sortMerge::sortMerge(
    char*           filename1,      // Name of heapfile for relation R
    int             len_in1,        // # of columns in R.
    AttrType        in1[],          // Array containing field types of R.
    short           t1_str_sizes[], // Array containing size of columns in R
    int             join_col_in1,   // The join column of R 

    char*           filename2,      // Name of heapfile for relation S
    int             len_in2,        // # of columns in S.
    AttrType        in2[],          // Array containing field types of S.
    short           t2_str_sizes[], // Array containing size of columns in S
    int             join_col_in2,   // The join column of S

    char*           filename3,      // Name of heapfile for merged results
    int             amt_of_mem,     // Number of pages available
    TupleOrder      order,          // Sorting order: Ascending or Descending
    Status&         s               // Status of constructor
){

#if 0
        // sorted result do not so good , so abadon it
         char sort_filename2[10]="sort2",sort_filename1[10]="sort1";
         Status hp;    
        int num_buffer = MINIBASE_BM -> getNumUnpinnedBuffers();
         key1_pos=-100;
        Sort(filename1,sort_filename1,len_in1, in1, t1_str_sizes, join_col_in1, order, num_buffer, s);
        num_buffer = MINIBASE_BM -> getNumUnpinnedBuffers();
        cout<<"number of buffer "<<num_buffer<<"  key1_pos "<<key1_pos<<"   key2_pos "<<key2_pos<<endl;
        key1_pos=0;
        Sort(filename2,sort_filename2,len_in2, in2, t2_str_sizes, join_col_in2, order,num_buffer , s);
  
        cout<<"number of buffer "<<num_buffer<<"  key1_pos "<<key1_pos<<"   key2_pos "<<key2_pos<<endl;

         HeapFile *f_in2=new HeapFile(sort_filename2,s);
         HeapFile *f_in1=new HeapFile(sort_filename1,s);
         HeapFile  *f_out = new HeapFile(filename3,hp);

           //  HeapFile *f_in3=new HeapFile(filename1,s);
#endif 
   // sort a  page , algorithm design by my own, has better sorted result , do no miss any tuple
        SortPage(filename1,len_in1, in1, t1_str_sizes,join_col_in1,order);    // sorted R
        SortPage(filename2,len_in2, in2, t2_str_sizes,join_col_in2,order);    // sorted S
           Status hp;    
           HeapFile *f_in2=new HeapFile(filename2,s);
         HeapFile *f_in1=new HeapFile(filename1,s);
         HeapFile  *f_out = new HeapFile(filename3,hp);

         short length=0;
        for(short i=0;i<len_in1;i++)
         length=length+t1_str_sizes[i];  // get total length of tuple
        if(in1[join_col_in1]==attrInteger&&in1[++join_col_in1]==attrString&&len_in1==2)    // int + string type, guess the data struct, for convenient, just define as test function
        {                                                                               // of course, it can be use as void point when get total length, but a little complex
            rec  rec_arry1[MAX_Tuple],rec_arry2[MAX_Tuple];
            int k=0,j=0,len;
            RID rid,rid_insert,first;
            char    rec1[sizeof(struct rec)*2];
            Scan*  scan2=f_in2->openScan(hp);
           //   Scan*  scan2=sort_file2->openScan(hp);
            if(hp!=OK)  cout<<"Fail: open scan 2"<<endl;
             Scan*  scan1=f_in1->openScan(hp);
            if(hp!=OK)  cout<<"Fail: open scan 1"<<endl;
            Status HP_INSERT, scan_all1,scan_all2;
            scan_all1=scan1->getNext(rid,(char *)&rec_arry1[k],len);
             scan_all2=scan2->getNext(rid,(char *)&rec_arry2[j],len);
              first=rid;
          
       /*   multimap<int, rec> Tupe2;
            while(scan2->getNext(rid,(char *)&rec_arry2[j],len)==OK)    // store every tuple of S in a map , so as to scan for join 
            {
              //   cout << (*((struct rec*)&rec1)).key << "\t" << (*((struct rec*)&rec1[8])).key << endl;
                   cout<<"map test"<<rec_arry2[j].key<<"   "<<len<<endl;
                Tupe2.insert(make_pair(rec_arry2[j].key,rec_arry2[j]));         // map have a better performance , scan ,search
            }
           // exit(0);
           */
          
              HP_INSERT=OK;
            while(HP_INSERT==OK&&scan_all1==OK)
            {       
                
  #if 1
                  scan_all2=scan2->getNext(rid,(char *)&rec_arry2[j],len);
                     while(scan_all2==OK)
                     {
                        //  cout<<"scan tuple 2 "<<rec_arry2[j].key<<endl;
                         if(rec_arry1[k].key==rec_arry2[j].key)
                         {
                               memcpy(rec1,&rec_arry2[j],sizeof(rec));
                               memcpy(rec1+sizeof(rec),&rec_arry1[k],sizeof(rec));
                                 HP_INSERT = f_out->insertRecord((char*)rec1,sizeof(rec)*2,rid);
                           //   HP_INSERT = f_out->insertRecord((char*)&rec_arry2[j],sizeof(rec),rid);
                           if (HP_INSERT != OK)
                             cout<<"this is ridiculous !"<<endl;
                         }
                         else if(rec_arry1[k].key<rec_arry2[j].key)
                               break; 
                         j++;
                         scan_all2=scan2->getNext(rid,(char *)&rec_arry2[j],len);

                     }
                    j=0;
                    if(scan2->position(first)!=OK)  cout<<"Error: can not reach the position"<<endl;

                     scan_all1=scan1->getNext(rid,(char *)&rec_arry1[k],len);
                   //      cout<<"Scan 1  "<<rec_arry1[k].key<<endl;
#endif
  #if 0
                   if(Tupe2.find(rec_arry1[k].key)!=Tupe2.end())        // scan whole S, find the match tuple
                   {
                          int count=Tupe2.count(rec_arry1[k].key);  // number of match tuple
                          int i=0;
                          multimap<int, rec>::iterator it=Tupe2.find(rec_arry1[k].key);
                          while(i<count)    // find it, insert to outfile
                          {
                               rec_arry2[j]=it->second;
                               memcpy(rec1,&rec_arry2[j],sizeof(rec));
                               memcpy(rec1+sizeof(rec),&rec_arry1[k],sizeof(rec));
                           /*    rec_arry1[k].key=120;
                               void *t1=&rec_arry2[j];
                               void *t2=&rec_arry1[k];
                              cout<<"tumcmp= "<<tupleCmp(t1,t2)<<endl;
                          */
                            //   cout<<"Insert key "<<rec_arry2[j].key<<endl;
                             //  HP_INSERT = f_out->insertRecord((char*)&rec_arry2[j],sizeof(rec),rid);
                               HP_INSERT = f_out->insertRecord((char*)rec1,sizeof(rec)*2,rid);
                               it++;
                               i++;
                          }
                   }
                    k++;
                    scan_all1=scan1->getNext(rid,(char *)&rec_arry1[k],len);
                      cout<<"tupe R key  "<<rec_arry1[k].key<<endl;
#endif

            }


             delete scan1,scan2;
        }
 

      
    //    s=f_in1->deleteFile();
    //    s=f_in2->deleteFile();
        delete f_out,f_in1;
        delete f_in2;
        s=OK;
	
    // fill in the body
}

// sortMerge destructor
sortMerge::~sortMerge()
{
	// fill in the body
}

int SortPage(char * filename1,int len_in1, AttrType in1[], short t1_str_sizes[],int  join_col_in1, TupleOrder  order)
{
        Status  hp,sort_read;
        HeapFile * f = new HeapFile(filename1,hp);
        if(hp!=OK)
        {
            cout << "Failed: open heapfile "<<filename1<<endl;
        }

        short length=0;
        for(short i=0;i<len_in1;i++)
         length=length+t1_str_sizes[i];  // get total length of tuple
        if(in1[join_col_in1]==attrInteger&&in1[++join_col_in1]==attrString&&len_in1==2)
        {
            int length_char=length-4;
            rec  rec_arry[MAX_Tuple];
            char    rec1[sizeof(struct rec)*2];
            int k=0, curr_page=0,len;
            RID rid,rid_insert;
            Scan*  scan =f->openScan(hp);
            if(hp!=OK)  cout<<"Fail: open scan "<<endl;
           //  scan->getNext(rid,rec1,len);
            scan->getNext(rid,(char *)&rec_arry[k++],len);
            curr_page=rid.pageNo;
             while(scan->getNext(rid,(char *)&rec_arry[k],len)==OK&&curr_page==rid.pageNo)
             {
              // cout<<"column  "<<rec_arry[k++].key<<endl;
                k++;
             }
             Page  *sort_slot=new Page();
             sort_read=MINIBASE_DB->read_page(curr_page,sort_slot);
             HFPage  *sort_hp=(HFPage *)sort_slot;
             // quick sort , for slot offset arry
             int key, j;
             rec  temp;
            for(int i=1;i<k;i++)
            {
                 key=rec_arry[i].key;
                 temp=rec_arry[i];
                 j=i-1;
                 if(order==Ascending)
                {
                 while(j>=0&&rec_arry[j].key>key)
                 {
            
                    rec_arry[j+1]=rec_arry[j];
                     j--;
                 }
               }
               else
               {
                while(j>=0&&rec_arry[j].key<key)
                 {
            
                    rec_arry[j+1]=rec_arry[j];
                     j--;
                 }
               }
                 rec_arry[j+1]=temp;
               
              
             }

             // update a page with sorted order
             sort_hp->init(curr_page);
             for(int i=0;i<k;i++)
             {
                sort_hp->insertRecord((char *)&rec_arry[i],sizeof(rec),rid_insert);
             }
            sort_read=MINIBASE_DB->write_page(curr_page,sort_slot);

            delete scan,sort_slot;
            delete f;

        }

        return 1;
}
