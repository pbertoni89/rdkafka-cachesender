#include <stdio.h>
#include <stdlib.h>

#define TABLE_LENGTH 1000000
#define RAND_SEED 0x1441222UL
#define MAX_WORD_LENGTH 12UL

int main(int argc, char *argv[]) 
{
    if(argc < 2)
	{
		fprintf(stderr, "Usage: wordgenerator word_number [feed]\n");
		exit(-1);
	}

    int word_number = atoi(argv[1]);
    if(word_number <= 0)
	{
        fprintf(stderr, "Invalid number of words\n");
        exit(-1);
    }

    long seed = RAND_SEED;
    if(argc == 3)
        seed = atoi(argv[2]);
    srand48(seed);

    int kk, jj;
    int word_length;
    char word_buffer[MAX_WORD_LENGTH + 1];

    for(kk = 0; kk < word_number; kk ++)
	{
        do
            word_length = lrand48() % MAX_WORD_LENGTH;
        while (word_length == 0);

        for(jj = 0; jj < word_length; jj ++)
		{
            char letter = (lrand48() % 25) + 'a';
            word_buffer[jj] = letter;
        }
        word_buffer[jj] = 0;

        fprintf(stdout, "%s ", word_buffer);
    }
    return 0;
}
