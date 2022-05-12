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

#pragma once
#ifndef INCLUDE_HMACFILTER_H
#define INCLUDE_HMACFILTER_H
#include <vector>
#include <bitset>
#include "HMAC.h"

#define REMAINDER 3
#define SIGN_S 6
#define BYTE_S 8
#define BFVAL_S 22
#define BF_S 64
#define MD5_S 128
#define SHA1_S 160


class HMACFilter
{
public:
	HMACFilter(HMAC *handler);
	~HMACFilter();

	int insert(unsigned char value[], int length);
	bool contains(unsigned char value[], int length);
	void clear();
	float getFalseRate();
	unsigned int getElemCount();
	unsigned char* getCanMessage();
	std::vector<bool> getBloomFilter();

	static void test();

private:

	std::vector<bool> bloomFilter;
	unsigned char *canMessage;
	HMAC *hmacHandler;
	unsigned int elemCount;

	template<size_t N>
	char binToInt(std::bitset<N> bitArr);
	void canFilter();

};
#endif
