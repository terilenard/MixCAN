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

#include "HMACFilter.h"
#include <iostream>
using namespace std;


HMACFilter::HMACFilter(HMAC *hmacHandler)
{
	this->elemCount = 0;
	this->hmacHandler = hmacHandler;

	this->bloomFilter.reserve(BF_S);
	this->bloomFilter.assign(BF_S, false);
}

HMACFilter::~HMACFilter()
{
	this->clear();
}

int HMACFilter::insert(const void *data, int dataLen)
{
	int count = 0;
	int retVal = 0;

	std::bitset<SIGN_S> signSet;
	std::bitset<BYTE_S> bitSet;
	std::vector<bool> binVector;

	if (!this->hmacHandler)
		return 1;

	char *hashOut = this->hmacHandler->sign(data, dataLen);
	
	for (int i = 0; i < strlen(hashOut); i++)
	{
		bitSet = std::bitset<BYTE_S>(hashOut[i]);

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

	signSet.reset();
	bitSet.reset();
	binVector.clear();

	return 0;
}

bool HMACFilter::contains(const void *data, int dataLen)
{
	bool exists = true;
	int count = 0;
	int retVal = 0;
	std::bitset<SIGN_S> signSet;
	std::bitset<BYTE_S> bitSet;
	std::vector<bool> binVector;

	if (!this->hmacHandler)
		return 1;

	char *hashOut = this->hmacHandler->sign(data, dataLen);

	for (int i = 0; i < strlen(hashOut); i++)
	{
		bitSet = std::bitset<BYTE_S>(hashOut[i]);

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


unsigned int HMACFilter::getElemCount()
{
	return this->elemCount;
}

unsigned char* HMACFilter::getCanMessage()
{
	return this->canFilter();
}

std::vector<bool> HMACFilter::getBloomFilter()
{
	return this->bloomFilter;
}

void HMACFilter::clear()
{
	this->bloomFilter.clear();
}

unsigned char* HMACFilter::canFilter()
{
	unsigned char *canMessage = new unsigned char(BYTE_S);

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

		canMessage[canCounter++] = this->binToInt(bitSet);
		bitSet.reset();
	}

	return canMessage;
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


int HMACFilter::bloomInBloom(unsigned char bloomFilter[], unsigned char queryFilter[])
{
	std::vector<bool> bloomVector;
	std::vector<bool> queryVector;

	std::bitset<BYTE_S> tmp;

	for (int i = 0; i < BYTE_S; i++)
	{
		tmp = std::bitset<BYTE_S>(bloomFilter[i]);

		for (int j = 0; j < BYTE_S; j++) {
			bloomVector.emplace_back((bool)tmp[j]);
		}
	}

	for (int i = 0; i < BYTE_S; i++)
	{
		tmp = std::bitset<BYTE_S>(queryFilter[i]);

		for (int j = 0; j < BYTE_S; j++) {
			queryVector.emplace_back((bool)tmp[j]);
		}
	}
	
	for (int i = 0; i < BF_S; i++)
	{
		if (queryVector[i] == true && queryVector[i] != bloomVector[i])
                {
			return 0;
                }
	}
	return 1;
}
