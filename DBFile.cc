#include "Errors.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "Heap.h"
#include <fstream>
#include <string.h>

DBFile *db;

DBFile::DBFile () {
    db = NULL;
}

DBFile::~DBFile () {

}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    printf("DBFile::Create\n");
    //FATALIF(db!=NULL, "File already opened.");
    CHECKOPENFILE(db);
    //createFile(f_type);
    
    // create file function below --------
    
    printf("creating file now\n");
    if(f_type == heap){
        db = new Heap();
    }
    else if(f_type == sorted){
        // to implement
    }
    else{
        db=NULL;
    }
    
    CHECKFILETYPE(db);
    
    
    //-----------------------------
    char *fPath = strdup(f_path);
    db->file.Open(0, fPath);
    isFileOpen = true;
    return 1;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    //printf("DBFile::LOAD\n");
    // new implementation -----------------------------------------
    
    if (!isFileOpen){
        cerr << "Trying to load a file which is not open!";
        exit(1);
    }
    
    
    if (mode == read ) {
        if( page.getNumRecs() > 0){
            page.EmptyItOut();
        }
    }
    
    if(mode!=write){
        mode = write;
    }
    
    // ------------------------------------------------------------
    FILE* input_file = fopen(loadpath, "r");
    //FATALIF(input_file==NULL, loadpath);
    
    CHECKOPENFILEPATH(input_file,loadpath);
    Record nextRecord;
    db->page.EmptyItOut();
    while (nextRecord.SuckNextRecord(&f_schema, input_file)) {
        Add(nextRecord);
    }
}

int DBFile::Open (const char* fpath) {
    printf("DBFile::OPEN\n");
    //FATALIF(db!=NULL, "File already opened.");
    CHECKOPENFILE(db);
    //int ftype = heap;
    
    // my implementation of ifstream--------
    int f_type_string=heap;
    fType f_type;
    ifstream ifs;
    std::string filePath= (db->parseTableName(fpath)).c_str();
    ifs.open(filePath);
    if(ifs.is_open()){
        ifs >> f_type_string;
        ifs.close();
    }
    f_type=static_cast<fType>(f_type_string);
    //-------------------------------------
    
    /*
    ifstream ifs((db->parseTableName(fpath)).c_str());
    if (ifs) {
        ifs >> ftype;
        ifs.close();
    }
    */
    //createFile(static_cast<fType>(ftype));
    
    // create file implementation below---
    
     printf("creating file now\n");
    if(f_type == heap){
        db = new Heap();
    }
    else if(f_type == sorted){
        // to implement
    }
    else{
        db=NULL;
    }
    
    CHECKFILETYPE(db);
    
    
    //----------------------------------------
    
    
    char *fPath = strdup(fpath);
    db->file.Open(1, fPath);
    isFileOpen = true;
    return 1;
}

void DBFile::MoveFirst () {
    printf("DBFile::MoveFirst\n");
    db->MoveFirst();
}

int DBFile::Close () {
    if (!isFileOpen) {
        cout << "trying to close a file which is not open!"<<endl;
        return 0;
    }
    printf("DBFile::Close\n");
    isFileOpen = false;
    db->Close();
}

void DBFile::Add (Record &rec) {
    //printf("DBFile::ADD,");
    if (!isFileOpen){
        cerr << "Trying to load a file which is not open!";
        exit(1);
    }
    db->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
    //printf("DBFile::GET_NEXT\n");
    while (!db->page.GetFirst(&fetchme)) {
        if(++db->pageIdx > db->file.lastIndex()) {
            return 0;
        }
        db->file.GetPage(&db->page, db->pageIdx);
    }
    return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    //printf("DBFile::GET_NEXT__\n");
    return db->GetNext(fetchme, cnf, literal);
}

/*
void DBFile::createFile(fType ftype) {
    printf("DBFile::createFILE\n");
    switch (ftype) {
        case heap: db = new Heap(); break;
        case sorted: break;
        default: db = NULL;
    }
    //FATALIF(db==NULL, "File Type is Invalid.");
    CHECKFILETYPE(db);
}
*/
