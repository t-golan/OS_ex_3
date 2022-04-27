//
// Created by אילון on 26/04/2022.
//

#include <pthread.h>
#include "MapReduceFramework.h"
#include <iostream>

#define SYSTEM_ERROR "system error: "

using namespace std;

struct JobContext{
    JobState jobState; // the job state
    pthread_mutex_t jobStateMutex; // a mutex to be used when interested in changing the jobState

    const MapReduceClient* client; // the given client
    const InputVec* inputVec; // the input vector
    OutputVec* outputVec; // the output vector
    int multiThreadLevel; // the amount of needed thread (maybe useless)
    pthread_t* threads; // pointer to an array of all existing threads
};

atomic<int> intermediaryElements(0); // a count for the amount of intermediary elements
atomic<int> outputElements(0); // a count for the amount of output elements
atomic<int> atomic_counter(0); // a generic count to be used

void emit2 (K2* key, V2* value, void* context){

    IntermediateVec* intermediateVec = (IntermediateVec*) context;
    IntermediatePair kv2 = IntermediatePair(key, value);
    intermediateVec->push_back(kv2);
    intermediaryElements++;
}

/***
 * updates the percentage of the job state
 * @param jobContext
 */
void updatePercentage(JobContext* jobContext){

    // the jobState is shared by all threads which makes changing it a critical code segment
    pthread_mutex_lock(&jobContext->jobStateMutex);

    if(jobContext->jobState.stage == MAP_STAGE){
        jobContext->jobState.percentage = intermediaryElements / jobContext->multiThreadLevel * 100;
        return;
    }
    if(jobContext->jobState.stage == REDUCE_STAGE){
        jobContext->jobState.percentage = outputElements / jobContext->multiThreadLevel * 100;
        return;
    }
    // need to add what happens in the shuffle case

    pthread_mutex_unlock(&jobContext->jobStateMutex);
}

/**
 * the map phase as it is suppose to be in all different threads (including the main thread)
 * @param arg the jobContext
 * @param context the context of the thread
 * @return Null
 */
void* mapPhase(void* arg, void* context){

    JobContext* jc = (JobContext*) arg;
    int oldValue = atomic_counter++;
    if(oldValue < jc->inputVec->size()) {
        InputPair kv = (*(jc->inputVec))[oldValue];
        jc->client->map(kv.first, kv.second, context);
        updatePercentage(jc);

    }
    return NULL;
}

/***
 * a thread - which is not the main one - this thread should:
 * map - sort - wait for shuffle - than reduce
 * @param arg a pointer to the jobContext
 * @return
 */
void* mapSortReduceThread(void* arg){

    OutputVec* outputMapVec = new OutputVec();

    // the map phase
    mapPhase(arg, outputMapVec);

}

/***
 * init the job context of the current job
 * @param client
 * @param inputVec
 * @param outputVec
 * @param multiThreadLevel
 * @param jobContext
 */
void initJobContext(const MapReduceClient& client,
                    const InputVec& inputVec, OutputVec& outputVec,
                    int multiThreadLevel, JobContext* jobContext){

    (*jobContext).multiThreadLevel = multiThreadLevel;
    (*jobContext).client = &client;
    (*jobContext).inputVec = &inputVec;
    (*jobContext).outputVec = &outputVec;
    (*jobContext).jobStateMutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_t threads[multiThreadLevel];
    (*jobContext).threads = threads;
}


JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){

    //init JobContext
    struct JobContext  jobContext;

    for (int i = 0; i < multiThreadLevel; ++i) {
        if(pthread_create(jobContext.threads + i, NULL, mapSortReduceThread, &jobContext) !=  0){
            cerr << SYSTEM_ERROR << "pthread_create";
            exit(1);
        }
    }

    // map phase of  the main thread

    // here should be the  barrier

    // the shuffle phase

    // sort phase
}

