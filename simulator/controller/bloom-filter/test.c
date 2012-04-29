/* Copyright (c) 2012 the authors listed at the following URL, and/or
the authors of referenced articles or incorporated external code:
http://en.literateprograms.org/Bloom_filter_(C)?action=history&offset=20111019004917

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Retrieved from: http://en.literateprograms.org/Bloom_filter_(C)?oldid=17386
*/


#include<stdio.h>
#include<string.h>

#include"bloom.h"

unsigned int sax_hash(const char *key)
{
	unsigned int h=0;

	while(*key) h^=(h<<5)+(h>>2)+(unsigned char)*key++;

	return h;
}

unsigned int sdbm_hash(const char *key)
{
	unsigned int h=0;
	while(*key) h=(unsigned char)*key++ + (h<<6) + (h<<16) - h;
	return h;
}

int main(int argc, char *argv[])
{
	FILE *fp;
	char line[1024];
	char *p;
	BLOOM *bloom;
	
	if(argc<2) {
		fprintf(stderr, "ERROR: No word file specified\n");
		return EXIT_FAILURE;
	}

	if(!(bloom=bloom_create(2500000, 2, sax_hash, sdbm_hash))) {
		fprintf(stderr, "ERROR: Could not create bloom filter\n");
		return EXIT_FAILURE;
	}

	if(!(fp=fopen(argv[1], "r"))) {
		fprintf(stderr, "ERROR: Could not open file %s\n", argv[1]);
		return EXIT_FAILURE;
	}

	while(fgets(line, 1024, fp)) {
		if((p=strchr(line, '\r'))) *p='\0';
		if((p=strchr(line, '\n'))) *p='\0';

		bloom_add(bloom, line);
	}

	fclose(fp);

	while(fgets(line, 1024, stdin)) {
		if((p=strchr(line, '\r'))) *p='\0';
		if((p=strchr(line, '\n'))) *p='\0';

		p=strtok(line, " \t,.;:\r\n?!-/()");
		while(p) {
			if(!bloom_check(bloom, p)) {
				printf("No match for ford \"%s\"\n", p);
			}
			p=strtok(NULL, " \t,.;:\r\n?!-/()");
		}
	}

	bloom_destroy(bloom);

	return EXIT_SUCCESS;
}
