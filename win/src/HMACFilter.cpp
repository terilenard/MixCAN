/*
* This work is licensed under the terms of the MIT license.  
* For a copy, see <https://opensource.org/licenses/MIT>.
*
* Developed by NISLAB - Network and Information Security Laboratory
* at George Emil Palade University of Medicine, Pharmacy, Science and
* Technology of Târgu Mureş <https://nislab.umfst.ro/>
*
* Contributors: Teri Lenard
*/

#include <assert.h>
#include <iostream>

#include "HMACFilter.h"


HMACFilter::HMACFilter(HMAC *handler)
{
	this->hmacHandler =  handler;
	this->elemCount = 0;

	this->bloomFilter.reserve(BF_S);
	this->bloomFilter.assign(BF_S, false);

	this->canMessage = new unsigned char(BYTE_S);
}

HMACFilter::~HMACFilter()
{
	this->clear();
}

int HMACFilter::insert(unsigned char value[], int length)
{
	int count = 0;
	int retVal = 0;

	std::bitset<SIGN_S> signSet;
	std::bitset<BYTE_S> bitSet;
	std::vector<bool> binVector;

	if (!this->hmacHandler)
		return 1;

	retVal = hmacHandler->sign(value, length);

	if (retVal != 0)
		return 2;

	for (DWORD i = 0; i < hmacHandler->length(); i++)
	{
		bitSet = std::bitset<BYTE_S>(hmacHandler->pbHash[i]);

		for (int j = 0; j < BYTE_S; j++) {
			binVector.emplace_back((bool)bitSet[j]);
		}
	}

	while (count < MD5_S)
	{
		if (count < MD5_S - REMAINDER)
		{
			for (int i = 0; i < SIGN_S; i++)
			{
				signSet.set(i, binVector[count]);
				count++;
			}
		}
		else
		{
			for (int i = 0; i < REMAINDER; i++)
			{
				signSet.set(i, binVector[count - 1]);
				count++;
			}
		}
		unsigned int entry = this->binToInt(signSet);
		this->bloomFilter[entry] = true;
		signSet.reset();
	}

	this->elemCount++;
	this->canFilter();

	signSet.reset();
	bitSet.reset();
	binVector.clear();

	return 0;
}

bool HMACFilter::contains(unsigned char value[], int length)
{
	bool exists = true;
	int count = 0;
	int retVal = 0;
	std::bitset<SIGN_S> signSet;
	std::bitset<BYTE_S> bitSet;
	std::vector<bool> binVector;

	if (!this->hmacHandler)
		return 1;

	retVal = hmacHandler->sign(value, length);

	if (retVal != 0)
		return 2;

	for (DWORD i = 0; i < hmacHandler->length(); i++)
	{
		bitSet = std::bitset<BYTE_S>(hmacHandler->pbHash[i]);

		for (int j = 0; j < BYTE_S; j++) {
			binVector.emplace_back((bool)bitSet[j]);
		}
	}

	while (count < MD5_S)
	{
		if (count < MD5_S - REMAINDER)
		{
			for (int i = 0; i < SIGN_S; i++)
			{
				signSet.set(i, binVector[count]);
				count++;
			}
		}
		else
		{
			for (int i = 0; i < REMAINDER; i++)
			{
				signSet.set(i, binVector[count - 1]);
				count++;
			}
		}
		unsigned int entry = this->binToInt(signSet);
		
		if (this->bloomFilter[entry] != true)
		{
			exists = false;
		}
		
		signSet.reset();
	}

	signSet.reset();
	bitSet.reset();
	binVector.clear();

	return exists;
}

float HMACFilter::getFalseRate()
{
	return std::pow(1 - std::exp(-BFVAL_S* ((float)elemCount) / ((float)BF_S)), BFVAL_S);
}

unsigned int HMACFilter::getElemCount()
{
	return this->elemCount;
}

unsigned char* HMACFilter::getCanMessage()
{
	return this->canMessage;
}

std::vector<bool> HMACFilter::getBloomFilter()
{
	return this->bloomFilter;
}

void HMACFilter::clear()
{
	if (this->hmacHandler)
	{
		delete hmacHandler;
	}

	this->bloomFilter.clear();

}

void HMACFilter::canFilter()
{
	std::bitset<BYTE_S> bitSet;
	int bfCounter = 0;
	int canCounter = 0;

	while (bfCounter < BF_S)
	{
		for (int i = 0; i < BYTE_S; i++)
		{
			bitSet.set(i, this->bloomFilter[bfCounter]);
			bfCounter++;
		}

		this->canMessage[canCounter++] = this->binToInt(bitSet);
		bitSet.reset();
	}
}

template<size_t N>
char HMACFilter::binToInt(std::bitset<N> bitArr)
{
	char bin;
	int i;

	for (bin = 0, i = 0; i < N; ++i)
	{
		bin *= 2;
		bin = bin + bitArr[i];
	}

	return bin;
}

void HMACFilter::test()
{
	bool exists = false;
	int retVal = 0;

	std::bitset<6> signSet;
	std::vector<bool> binVect;

	unsigned char secret[9] = { 0x70, 0x61, 0x73, 0x73, 0x77, 0x6F, 0x72, 0x64, 0x12 };
	unsigned char message1[8] = { 0x06, 0x08, 0x09, 0x0a, 0x09, 0x08, 0x0a, 0x01 };
	unsigned char message2[3] = { 0x12, 0x3, 0xc };
	unsigned char message3[1] = { 0x12 };
	unsigned char message4[1] = { 0x22 };
	unsigned char message5[2] = { 0x22 , 0x45};

	char *canMessage;
	
	HMAC* hmacHandler = new HMAC(secret, 9);
	
	retVal = hmacHandler->setup();
	assert(retVal == 0);

	retVal = hmacHandler->sign(message1, 8);
	assert(retVal == 0);

	HMACFilter* hmacFilter = new HMACFilter(hmacHandler);
	assert(hmacFilter != nullptr);
	assert(hmacFilter->getElemCount() == 0);

	retVal = hmacFilter->insert(message1, 8);
	assert(retVal == 0);
	assert(hmacFilter->getElemCount() == 1);

	retVal = hmacFilter->insert(message2, 3);
	assert(retVal == 0);
	assert(hmacFilter->getElemCount() == 2);

	retVal = hmacFilter->insert(message3, 1);
	assert(retVal == 0);
	assert(hmacFilter->getElemCount() == 3);

	retVal = hmacFilter->insert(message4, 1);
	assert(retVal == 0);
	assert(hmacFilter->getElemCount() == 4);

	exists = hmacFilter->contains(message1, 8);
	assert(exists == true);

	exists = hmacFilter->contains(message2, 3);
	assert(exists == true);

	exists = hmacFilter->contains(message3, 1);
	assert(exists == true);

	exists = hmacFilter->contains(message4, 1);
	assert(exists == true);

	exists = hmacFilter->contains(message5, 2);
	assert(exists == false);

	printf("\n %d", hmacFilter->getElemCount());
}
