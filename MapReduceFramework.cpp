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
    pthread_mutex_t waitMutex = PTHREAD_MUTEX_INITIALIZER;
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
    while(oldValue < jc->inputVec->size()) {
        InputPair kv = (*(jc->inputVec))[oldValue];
        jc->client->map(kv.first, kv.second, context);
        updatePercentage(jc);
    }
}

void sortPhase(void* context){
    ThreadContext* tc = (ThreadContext*) context;
    sort(tc->intermediateVec->begin(), tc->intermediateVec->end());
}


void shufflePhase(void* arg) {

    JobContext *jc = (JobContext *) arg;
    auto shuffleVec = jc->intermediateVec;
    IntermediateVec* current_iv = new IntermediateVec();
    jc->intermediateVec.push_back(*current_iv);
    ThreadContext* contextsOfThreads =  jc->contexts;
    int numOfEmptyVectors = 0;
    bool is_first = true; // first vector to be created

    while (numOfEmptyVectors < jc->multiThreadLevel){
        int i = 0;
        while(contextsOfThreads[i].intermediateVec->empty()){i++;} // finds first not empty vector
        auto max = *(contextsOfThreads[i].intermediateVec->begin()); // sets the max
        auto prev_max  = max;
        int max_index = i;
        for(int j=i; j < jc->multiThreadLevel; j++){
            if(*(contextsOfThreads[j].intermediateVec->begin()) > max){
                max = *(contextsOfThreads[j].intermediateVec->begin());
                max_index = j;

            }
        }
        // if it's the first vector
        if(is_first){
            current_iv->push_back(max);
            prev_max = max;
            contextsOfThreads[max_index].intermediateVec->pop_back();
            is_first  = false;
        }
        // if it's not the first vector
        else{
            // if we should create a new vector
            if(prev_max != max){
                current_iv = new IntermediateVec();
                jc->intermediateVec.push_back(*current_iv);
                current_iv->push_back(max);
                contextsOfThreads[max_index].intermediateVec->pop_back();
                prev_max = max;
            }
            // if we can keep use the current vector
            else{
                current_iv->push_back(max);
                contextsOfThreads[max_index].intermediateVec->pop_back();
            }
        }

        // if this iteration made one of the old vectors empty
        if(contextsOfThreads[max_index].intermediateVec->empty()){
            numOfEmptyVectors++;
        }
    }
}

void reducePhase(void* arg, void* context){
    JobContext* jc = (JobContext*) arg;
    int oldValue = *(jc->atomic_counter)++;
    while(oldValue < jc->intermediateVec.size()) {
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
    if(++(*(jc->atomic_barrier)) == jc->multiThreadLevel) // indicates sort phase of this thread is over
    {
        // declares all threads finished the sort phase
        if (pthread_cond_broadcast(&(jc->cvMapSortBarrier)) != 0) {
            cerr << SYSTEM_ERROR << "pthread_cond_broadcast MapSort";
            exit(1);
        }
    }
    if(pthread_cond_wait(&(jc->cvShuffleBarrier), NULL) != 0){
        cerr << SYSTEM_ERROR << "pthread_cond_wait shuffle";
        exit(1);
    }
    reducePhase(arg, &threadContext);
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

}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    //init JobContext
    struct JobContext jobContext;
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
        if(pthread_cond_wait(&(jobContext.cvMapSortBarrier), NULL) != 0) {
            cerr << SYSTEM_ERROR << "pthread_cond_wait mapSortBarrier main thread";
            exit(1);
        }
    }
    jobContext.atomic_counter = 0;
    shufflePhase(&jobContext);

    if (pthread_cond_broadcast(&(jobContext.cvShuffleBarrier)) != 0) {
        cerr << SYSTEM_ERROR << "pthread_cond_broadcast ShuffleBarrier main thread";
        exit(1);
    }
    reducePhase(&jobContext, &mainThread);

}

void getJobState(JobHandle job, JobState* state){
    auto jc = (JobContext*) job;
    state->stage = jc->jobState.stage;
    state->percentage = jc->jobState.percentage;
}

void waitForJob(JobHandle job){
    auto jc = (JobContext*) job;
    if(pthread_mutex_lock(&jc->waitMutex) != 0){
        std::cerr << "Mutex lock error";
        exit(1);
    }
    for(int i=0; i<jc->multiThreadLevel; ++i){
        if(pthread_join(jc->threads[i], nullptr) != 0){
            cerr << SYSTEM_ERROR << "pthread_join waitForJob " << i << " thread";
            exit(1);
        }
    }
    pthread_mutex_unlock(&jc->waitMutex);
}

void closeJobHandle(JobHandle job){
    waitForJob(job);
    auto jc = (JobContext*) job;
    delete &jc->intermediateVec;
    delete jc;
}
