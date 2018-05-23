// ... ο κώδικάς σας για την υλοποίηση του quicksort
// με pthreads και thread pool...
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define HAVE_STRUCT_TIMESPEC
#include <pthread.h>

#define ARRAY_SIZE	   10000000 // 10M doubles
#define CUTOFF			  1000
#define THREAD_POOL			 4
#define QUEUE_SIZE		  5000

double* MakeNumbers(int size);
void DeleteNumbers(double* numbers);
void FillNumbers(double* numbers, int size);
void CheckNumbers(double* numbers, int size);

// This message type appears when a thread is trying to
// get the next message from the queue but there is none.
// Sender {-}		Receiver{Main/Worker}
#define MESSAGE_INVALID		-1

// This message gives the task of partitioning a numbers[from,to] block
// from the 'numbers' array. When this operation is done, 
// the same thread places two other 'MESSAGE_PARTITION' messages into
// the queue. Also, if the [from,to] block size is less than the cutoff
// value, it places a 'MESSAGE_QSORT' message instead.
// Sender {Main/Worker}		Receiver{Worker}
#define MESSAGE_PARTITION	 1

// This message signals that a quicksort must be performed
// to the [from,to] number block. The thread performing it
// sends a 'MESSAGE_DONE' signal to the main thread when
// finished.
// Sender {Worker}		Receiver{Worker}
#define MESSAGE_QSORT		 2

// The sorted block's size is sent to  the main thread 
// so it can keep track of the work done.
// Sender {Worker}		Receiver{Main}
#define MESSAGE_DONE		 3

// This message is sent from the main thread to all worker threads
// so they can exit their main function.
// The worker thread receiving this message, must put it back into 
// the queue so the other work threads can receive it too.
// Sender {Main}		Receiver{Worker}
#define MESSAGE_SHUTDOWN	 10

typedef struct 
{
	int msg;
	double *numbers;
	int size;
} Message_t;



typedef struct 
{
	int front;
	int back;
	int size;
	Message_t *messages;
} WorkQueue_t;


// Queue operations:
WorkQueue_t* QueueMake(int size);
void         QueueDelete(WorkQueue_t *queue);
int          QueueIsEmpty(WorkQueue_t *queue);
int          QueueIsFull(WorkQueue_t *queue);
void         QueueInsert(WorkQueue_t *queue, Message_t msg);
Message_t    QueuePop(WorkQueue_t *queue);


// Global work queue:
WorkQueue_t* GlobalWorkQueue = NULL;


// Taken from: https://gist.github.com/mixstef/322145437c092783f70f243e47769ac6
void SeqSort(double *numbers, int size);

// condition variable, signals a put operation (receiver waits on this)
pthread_cond_t msg_in = PTHREAD_COND_INITIALIZER;
// condition variable, signals a get operation (sender waits on this)
pthread_cond_t msg_out = PTHREAD_COND_INITIALIZER;

// mutex protecting common resources
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;




// work thread function:
void *WorkerThread(void *args) 
{
	int shutdown = 0;

	while (!shutdown)
	{
		// Lock mutex:
		pthread_mutex_lock(&mutex);

		// Wait until there is a message in the work queue:
		while(QueueIsEmpty(GlobalWorkQueue))
		{	// NOTE: we use while instead of if! more than one thread may wake up
			pthread_cond_wait(&msg_in, &mutex);
		}

		// If there is, get it:
		Message_t msg = QueuePop(GlobalWorkQueue);

		// signal the sender that something was removed from buffer
		pthread_cond_signal(&msg_out);

		// unlock mutex
		pthread_mutex_unlock(&mutex);

		// Process message:
		switch (msg.msg)
		{
			// Do nothing:
			case MESSAGE_INVALID:
			{
				;
			}
			break;

			// Split the [from,to] block into two parts and
			// place two messages into the queue (or one if below cutoff):
			case MESSAGE_PARTITION:
			{
				if (msg.size <= CUTOFF)
				{
					// Send a 'MESSAGE_QSORT' signal:
					Message_t msgqsort;
					msgqsort = msg;
					msgqsort.msg = MESSAGE_QSORT;

					pthread_mutex_lock(&mutex);

					QueueInsert(GlobalWorkQueue, msgqsort);

					// signal the receiver that something was put in buffer
					pthread_cond_signal(&msg_in);

					// unlock mutex
					pthread_mutex_unlock(&mutex);
				}

				// perform partitioning (taken from: https://gist.github.com/mixstef/322145437c092783f70f243e47769ac6):
				else
				{
					double t = 0.0;
					double p = 0.0;
						
					int first = 0;
					int middle = msg.size - 1;
					int last = msg.size / 2;

					// put median-of-3 in the middle
					if (msg.numbers[middle]<msg.numbers[first])
					{ 
						t = msg.numbers[middle];
						msg.numbers[middle] = msg.numbers[first];
						msg.numbers[first] = t;
					}
					if (msg.numbers[last]<msg.numbers[middle])
					{ 
						t = msg.numbers[last];
						msg.numbers[last] = msg.numbers[middle];
						msg.numbers[middle] = t;
					}
					if (msg.numbers[middle]<msg.numbers[first])
					{ 
						t = msg.numbers[middle];
						msg.numbers[middle] = msg.numbers[first];
						msg.numbers[first] = t;
					}

					// partition (first and last are already in correct half)
					p = msg.numbers[middle]; // pivot
					int i, j;
					for (i = 1, j = msg.size-2; ; i++, j--) 
					{
						while (msg.numbers[i] < p)
						{
							i++;
						}
						while (p < msg.numbers[j])
						{
							j--;
						}
						if (i >= j)
						{
							break;
						}

						t = msg.numbers[i]; 
						msg.numbers[i] = msg.numbers[j]; 
						msg.numbers[j] = t;
					}

					// send two separate 'MESSAGE_PARTITION' messages:
					Message_t msgpart;
					msgpart.msg = MESSAGE_PARTITION;

					msgpart.numbers = msg.numbers;
					msgpart.size =  i;

					pthread_mutex_lock(&mutex);

					QueueInsert(GlobalWorkQueue, msgpart);

					msgpart.numbers = msg.numbers + i;
					msgpart.size = msg.size - i;

					QueueInsert(GlobalWorkQueue, msgpart);

					// signal the receiver that something was put in buffer
					pthread_cond_signal(&msg_in);

					// unlock mutex
					pthread_mutex_unlock(&mutex);

				}
			}
			break;

			//
			case MESSAGE_QSORT:
			{
				// Sort this block:
				SeqSort(msg.numbers, msg.size);

				// Send a 'MESSAGE_DONE' signal to the main thread:
				Message_t msgdone;
				msgdone = msg;
				msgdone.msg = MESSAGE_DONE;

				pthread_mutex_lock(&mutex);

				QueueInsert(GlobalWorkQueue, msgdone);

				// signal the receiver that something was put in buffer
				pthread_cond_signal(&msg_in);

				// unlock mutex
				pthread_mutex_unlock(&mutex);
			}
			break;

			// This message type is not meand for the worker thread.
			// Place it back into the queue:
			case MESSAGE_DONE:
			{
				pthread_mutex_lock(&mutex);

				QueueInsert(GlobalWorkQueue, msg);

				// signal the receiver that something was put in buffer
				pthread_cond_signal(&msg_in);

				// unlock mutex
				pthread_mutex_unlock(&mutex);
			}
			break;

			//
			case MESSAGE_SHUTDOWN:
			{
				// Place back this message into the queue:
				pthread_mutex_lock(&mutex);

				QueueInsert(GlobalWorkQueue, msg);

				// signal the receiver that something was put in buffer
				pthread_cond_signal(&msg_in);

				// unlock mutex
				pthread_mutex_unlock(&mutex);

				// Exit:
				shutdown = 1;
			}
			break;

			default:
			{
				printf("WorkThread: bad message type received.\n");
			}
		}

	}

	printf("WorkThread: shutting down.\n");

	// exit and let be joined
	pthread_exit(NULL);

	// return something so there aren't any compiler warnings:
	return NULL;
}




int main(int argc, char** argv)
{
	// Initialize an array of random numbers:
	double* numbers = MakeNumbers(ARRAY_SIZE);
	FillNumbers(numbers, ARRAY_SIZE);
	
	// Initialize work queue:
	GlobalWorkQueue = QueueMake(QUEUE_SIZE);

	if (GlobalWorkQueue == NULL)
	{
		printf("Queue: cannot be initialized.\n");
		exit(-1);
	}

	// Create the thread pool:
	pthread_t WorkerPool[THREAD_POOL];
	for (int i = 0; i < THREAD_POOL; i++)
	{
		pthread_create(&(WorkerPool[i]), NULL, WorkerThread, NULL);
	}

	// Send our first message to start the operation:
	// Lock mutex:
	pthread_mutex_lock(&mutex);

	// send message:
	Message_t msg_start;
	msg_start.msg = MESSAGE_PARTITION;
	msg_start.size = ARRAY_SIZE;
	msg_start.numbers = numbers;
	QueueInsert(GlobalWorkQueue, msg_start);

	// signal the receiver that something was put in buffer
	pthread_cond_signal(&msg_in);

	// unlock mutex
	pthread_mutex_unlock(&mutex);

	// wait until all work is done:
	int workIsDone = 0;
	int numbersSorted = 0;

	while (!workIsDone)
	{
		// scan for 'MESSAGE_DONE':
		// Lock mutex:
		pthread_mutex_lock(&mutex);

		// Wait until there is a message in the work queue:
		while (QueueIsEmpty(GlobalWorkQueue))
		{	// NOTE: we use while instead of if! more than one thread may wake up
			pthread_cond_wait(&msg_in, &mutex);
		}

		// If there is, get it:
		Message_t msg = QueuePop(GlobalWorkQueue);

		// signal the sender that something was removed from buffer
		pthread_cond_signal(&msg_out);

		// unlock mutex
		pthread_mutex_unlock(&mutex);

		// Process message:
		switch (msg.msg)
		{
			// Do nothing:
			case MESSAGE_INVALID:
			{
				;
			}
			break;

			// Put back:
			case MESSAGE_PARTITION:
			{
				// Place back this message into the queue:
				pthread_mutex_lock(&mutex);

				QueueInsert(GlobalWorkQueue, msg);

				// signal the receiver that something was put in buffer
				pthread_cond_signal(&msg_in);

				// unlock mutex
				pthread_mutex_unlock(&mutex);
			}
			break;

			// Put back:
			case MESSAGE_QSORT:
			{
				// Place back this message into the queue:
				pthread_mutex_lock(&mutex);

				QueueInsert(GlobalWorkQueue, msg);

				// signal the receiver that something was put in buffer
				pthread_cond_signal(&msg_in);

				// unlock mutex
				pthread_mutex_unlock(&mutex);
			}
			break;

			// Add all sorted blocks' sizes and determine if
			// all work is done:
			case MESSAGE_DONE:
			{
				numbersSorted += msg.size;

				workIsDone = (numbersSorted >= ARRAY_SIZE);
			}
			break;

			// Put back:
			case MESSAGE_SHUTDOWN:
			{
				// Place back this message into the queue:
				pthread_mutex_lock(&mutex);

				QueueInsert(GlobalWorkQueue, msg);

				// signal the receiver that something was put in buffer
				pthread_cond_signal(&msg_in);

				// unlock mutex
				pthread_mutex_unlock(&mutex);
			}
			break;

			default:
			{
				printf("MainThread: bad message type received.\n");
			}
		}

	}

	// send 'MESSAGE_SHUTDOWN' to all threads:
	// Lock mutex:
	pthread_mutex_lock(&mutex);

	Message_t msg_end;
	msg_end.msg = MESSAGE_SHUTDOWN;
	QueueInsert(GlobalWorkQueue, msg_end);

	// signal the receiver that something was put in buffer
	pthread_cond_signal(&msg_in);

	// unlock mutex
	pthread_mutex_unlock(&mutex);

	// Join threads from pool:
	for (int i = 0; i < THREAD_POOL; i++)
	{
		pthread_join(WorkerPool[i], NULL);
	}

	// destroy mutex - should be unlocked
	pthread_mutex_destroy(&mutex);

	// destroy cvs - no process should be waiting on these
	pthread_cond_destroy(&msg_out);
	pthread_cond_destroy(&msg_in);

	// Check the sorted array for errors:
	CheckNumbers(numbers, ARRAY_SIZE);

	// Deallocate the number array:
	DeleteNumbers(numbers);

	QueueDelete(GlobalWorkQueue);

	return 0;
}




double * MakeNumbers(int size)
{
	return (double*)malloc(sizeof(double) * size);
}




void DeleteNumbers(double * numbers)
{
	free(numbers);
}




void FillNumbers(double * numbers, int size)
{
	srand((unsigned int)time(NULL));
	for (int i = 0; i < size; i++)
	{
		numbers[i] = (double)rand();
	}
}




void CheckNumbers(double * numbers, int size)
{
	int correct = 1;

	for (int i = 1; i < size; i++)
	{
		if (numbers[i - 1] > numbers[i])
		{
			correct = 0;
			break;
		}
	}

	if (correct)
	{
		printf("Array was sorted properly.\n");
	}
	else
	{
		printf("Unable to sort array.\n");
	}
}



WorkQueue_t* QueueMake(int size)
{
	WorkQueue_t* ret = (WorkQueue_t*)malloc(sizeof(WorkQueue_t));

	if (ret == NULL)
	{
		return NULL;
	}
	ret->size = size;
	ret->front = -1;
	ret->back = -1;
	ret->messages = malloc(size * sizeof(Message_t));
	if (ret->messages==NULL)
	{
		return NULL;
	}
	return ret;
}




void QueueDelete(WorkQueue_t * queue)
{
	if (queue != NULL)
	{
		if (queue->messages != NULL)
		{
			free(queue->messages);
			queue->messages = NULL;
		}

		free(queue);
		queue = NULL;
	}
}




int QueueIsEmpty(WorkQueue_t *queue)
{
	return(queue->front == -1);
}




int QueueIsFull(WorkQueue_t *queue)
{
	if ((queue->front == 0 && queue->back == queue->size - 1) ||
		(queue->back == (queue->front - 1) % (queue->size - 1)))
	{
		return 1;
	}
	return 0;
}





void QueueInsert(WorkQueue_t *queue, Message_t msg)
{
	if (QueueIsFull(queue))
	{
		printf("Error: Queue overflow.\n");
	}
	else
	{
		if (queue->front == -1)
		{
			queue->front = queue->back = 0;
			queue->messages[queue->back] = msg;
		}

		else if (queue->back == queue->size - 1 && queue->front != 0)
		{
			queue->back = 0;
			queue->messages[queue->back] = msg;
		}

		else
		{
			queue->back++;
			queue->messages[queue->back] = msg;
		}
	}
}




Message_t QueuePop(WorkQueue_t *queue)
{
	Message_t msg;
	msg.msg = MESSAGE_INVALID;

	if (QueueIsEmpty(queue))
	{
		return msg;
	}

	msg = queue->messages[queue->front];
	if (queue->front == queue->back)
	{
		queue->front = -1;
		queue->back = -1;
	}
	else if (queue->front == queue->size - 1)
		queue->front = 0;
	else
		queue->front++;

	return msg;

}




void SeqSort(double *numbers, int size)
{
	int i, j;
	double t;

	for (i = 1; i<size; i++)
	{
		j = i;
		while ((j>0) && (numbers[j - 1]>numbers[j]))
		{
			t = numbers[j - 1];
			numbers[j - 1] = numbers[j];
			numbers[j] = t;
			j--;
		}
	}
}