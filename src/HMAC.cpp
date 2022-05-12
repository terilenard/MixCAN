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

#include "HMAC.h"


HMAC::HMAC(void *key, int keyLen)
{
	this->key = key;
	this->keyLen = keyLen;
	this->ctx = new MD5();
}

HMAC::~HMAC()
{
	if (this->ctx)
	{
		delete this->ctx;
	}

	if (this->key)
	{
		#pragma optimize("", off)
		memset(this->key, 0, this->keyLen);
		#pragma optimize("", on)
	}

}

char *HMAC::sign(const void *data, int dataLen)
{
	char *md5str = this->ctx->hmac_md5(data, dataLen, this->key, this->keyLen);
	return md5str;
}

