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
#ifndef INCLUDE_HMAC_H
#define INCLUDE_HMAC_H

#define HashMD5 1
#define HashSHA1 2

#include "MD5.h"

class HMAC
{

public:
	HMAC(void *key, int keyLen);
	~HMAC();

	char *sign(const void *data, int dataLen);

protected:
	void *key;
	int keyLen;
	MD5 *ctx;
};
#endif
