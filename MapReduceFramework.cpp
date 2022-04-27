//
// Created by אילון on 26/04/2022.
//

#include <pthread.h>
#include "MapReduceFramework.h"
#include <iostream>
#include <atomic>

#define SYSTEM_ERROR "system error: "

using namespace std;
struct ThreadContext{
    IntermediateVec* intermediateVec;
    atomic<int>* intermediaryElements;
    OutputVec* outputVec;
    atomic<int>* outputElements;
};

struct JobContext{
    JobState jobState; // the job state
    pthread_mutex_t jobStateMutex; // a mutex to be used when interested in changing the jobState

    const MapReduceClient* client; // the given client
    const InputVec* inputVec; // the input vector
    OutputVec* outputVec; // the output vector
    vector<IntermediateVec> intermediateVec;
    int multiThreadLevel; // the amount of needed thread (maybe useless)
    pthread_t* threads; // pointer to an array of all existing threads
    ThreadContext* contexts;

    atomic<int>* intermediaryElements; // a count for the amount of intermediary elements
    atomic<int>* outputElements; // a count for the amount of output elements
    atomic<int>* atomic_counter; // a generic count to be used3
    atomic<int>* atomic_barrier; // a counter to use to implement the barrier
    atomic<int>* threadsId; // gives an id to each thread


    pthread_mutex_t barrierMutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t cvMapSortBarrier = PTHREAD_COND_INITIALIZER;
    pthread_cond_t cvShuffleBarrier = PTHREAD_COND_INITIALIZER;
};


void emit2 (K2* key, V2* value, void* context){

    ThreadContext* threadContext = (ThreadContext*) context;
    IntermediatePair kv2 = IntermediatePair(key, value);
    threadContext->intermediateVec->push_back(kv2);
    threadContext->intermediaryElements++;
}

void emit3 (K3* key, V3* value, void* context){
    ThreadContext* threadContext = (ThreadContext*) context;
    OutputPair kv3 = OutputPair(key, value);
    threadContext->outputVec->push_back(kv3);
    threadContext->intermediaryElements++;
}

/***
 * updates the percentage of the job state
 * @param jobContext
 */
void updatePercentage(JobContext* jobContext){

    // the jobState is shared by all threads which makes changing it a critical code segment
    pthread_mutex_lock(&jobContext->jobStateMutex);

    if(jobContext->jobState.stage == MAP_STAGE){
        jobContext->jobState.percentage = *(jobContext->intermediaryElements) / jobContext->multiThreadLevel * 100;
        return;
    }
    if(jobContext->jobState.stage == REDUCE_STAGE){
        jobContext->jobState.percentage = *(jobContext->outputElements) / jobContext->multiThreadLevel * 100;
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
void mapPhase(void* arg, void* context){

    JobContext* jc = (JobContext*) arg;
    int oldValue = *(jc->atomic_counter)++;
    if(oldValue < jc->inputVec->size()) {
        InputPair kv = (*(jc->inputVec))[oldValue];
        jc->client->map(kv.first, kv.second, context);
        updatePercentage(jc);
    }
}

void sortPhase(void* context){
    ThreadContext* tc = (ThreadContext*) context;
    sort(tc->intermediateVec->begin(), tc->intermediateVec->end());
}


vector<IntermediateVec> shufflePhase(void* arg) {
    JobContext *jc = (JobContext *) arg;
    auto shuffleVec = new vector<IntermediateVec>();
    auto max = jc->contexts[0].intermediateVec->begin();
    auto prev_max = NULL;
    for(int i = 0; jc->multiThreadLevel; i++) {
        if (jc->contexts[0].intermediateVec->begin() > max) {
            max = jc->contexts[0].intermediateVec->begin();
        }
        if (prev_max != max){
            Vec->insert(new IntermediateVec());
        }
        shu
    }

        for (int i = 0; jc->multiThreadLevel; i++) {
            if (jc->contexts[0].intermediateVec->begin() == max) {
                shuffleVec.end(.insert())
            }
        }
    }
}

void reducePhase(void* arg, void* context){
    JobContext* jc = (JobContext*) arg;
    int oldValue = *(jc->atomic_counter)++;
    if(oldValue < jc->intermediateVec.size()) {
        IntermediateVec kv = ((jc->intermediateVec))[oldValue];
        jc->client->reduce(&kv, context);
        updatePercentage(jc);
    }
}

/***
 * a thread - which is not the main one - this thread should:
 * map - sort - wait for shuffle - than reduce
 * @param arg a pointer to the jobContext
 * @return
 */
void* mapSortReduceThread(void* arg){

    JobContext* jc = (JobContext*) arg;
    int id = ++(*(jc->threadsId));
    ThreadContext threadContext =  jc->contexts[id];
    threadContext.intermediateVec = new IntermediateVec();
    threadContext.outputVec = jc->outputVec;
    threadContext.intermediaryElements = jc->intermediaryElements;
    threadContext.outputVec = jc->outputVec;

    // the map phase
    mapPhase(arg, &threadContext);
    sortPhase(&threadContext);
    if(++(*(jc->atomic_barrier)) < jc->multiThreadLevel)
    {
        if(pthread_cond_wait(&(jc->cvShuffleBarrier), NULL) != 0){
            cerr << SYSTEM_ERROR << "pthread_cond_wait shuffle";
            exit(1);
        }
    }
    else{
        if (pthread_cond_broadcast(&(jc->cvMapSortBarrier)) != 0) {
            cerr << SYSTEM_ERROR << "pthread_cond_broadcast MapSort";
            exit(1);
        }
    }
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
    ThreadContext contexts[multiThreadLevel];
    (*jobContext).contexts = contexts;

    // the atomic_counters used by the job
    atomic<int> atomic_counter(0);
    atomic<int> atomic_barrier(0);
    atomic<int> intermediaryElements(0);
    atomic<int> outputElements(0);
    atomic<int> threadsId(0);

    (*jobContext).atomic_counter = &atomic_counter;
    (*jobContext).atomic_barrier = &atomic_barrier;
    (*jobContext).intermediaryElements = &intermediaryElements;
    (*jobContext).outputElements = &outputElements;
    (*jobContext).threadsId = &threadsId;

}


JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    //init JobContext
    struct JobContext  jobContext;
    initJobContext(client, inputVec, outputVec, multiThreadLevel, &jobContext);

    for (int i = 0; i < multiThreadLevel; ++i) {
        if(pthread_create(jobContext.threads + i, NULL, mapSortReduceThread, &jobContext) !=  0){
            cerr << SYSTEM_ERROR << "pthread_create";
            exit(1);
        }
    }

    ThreadContext mainThread;
    jobContext.contexts[0] = mainThread;
    IntermediateVec* intermediateVec = new IntermediateVec();

    mainThread.intermediateVec = intermediateVec;
    mainThread.outputVec = jobContext.outputVec;
    mainThread.intermediaryElements = jobContext.intermediaryElements;
    mainThread.outputElements = jobContext.outputElements;


    mapPhase(&jobContext, &mainThread);
    sortPhase(&mainThread);
    if(++(*(jobContext.atomic_barrier)) < jobContext.multiThreadLevel)
    {
        pthread_cond_wait(&(jobContext.cvMapSortBarrier), NULL);
    }
    jobContext.atomic_counter = 0;
    jobContext.intermediateVec = (shufflePhase(&jobContext));
    // the shuffle phase

}

