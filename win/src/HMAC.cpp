/*
* This work is licensed under the terms of the MIT license.  
* For a copy, see <https://opensource.org/licenses/MIT>.
*
* Developed by NISLAB - Network and Information Security Laboratory
* at George Emil Palade University of Medicine, Pharmacy, Science and
* Technology of Târgu Mureş <https://nislab.umfst.ro/>
*
* Contributors: Teri Lenard
*
* Source code was taken from: https://docs.microsoft.com/en-us/windows/win32/seccrypto/example-c-program--creating-an-hmac
*/

#include "HMAC.h"


HMAC::HMAC(unsigned char password[], int length)
{
	this->secretLength = length;

	secret = new BYTE[length];
	for (int i = 0; i < length; i++)
		secret[i] = password[i];

	// Zero the HMAC_INFO structure and use the SHA1 algorithm for hashing.
	ZeroMemory(&this->HmacInfo, sizeof(this->HmacInfo));
	HmacInfo.HashAlgid = CALG_MD5;

}

HMAC::~HMAC()
{
	if (this->hHmacHash)
		CryptDestroyHash(this->hHmacHash);
	if (this->hKey)
		CryptDestroyKey(this->hKey);
	if (this->hHash)
		CryptDestroyHash(this->hHash);
	if (this->hProv)
		CryptReleaseContext(this->hProv, 0);
	if (this->pbHash)
		free(this->pbHash);
}

int HMAC::setup()
{
	if (!CryptAcquireContext(
		&hProv,                   // handle of the CSP
		NULL,                     // key container name
		NULL,                     // CSP name
		PROV_RSA_FULL,            // provider type
		CRYPT_VERIFYCONTEXT))     // no key access is requested
	{
		return 1;
	}

	/* Derive a symmetric key from a hash object by performing the
	   following steps:
		 1. Call CryptCreateHash to retrieve a handle to a hash object.
		 2. Call CryptHashData to add a text string (password) to the
			hash object.
		 3. Call CryptDeriveKey to create the symmetric key from the
			hashed password derived in step 2.
	   You will use the key later to create an HMAC hash object.
	  */

	if (!CryptCreateHash(
		this->hProv,			  // handle of the CSP
		CALG_SHA1,                // hash algorithm to use
		0,                        // hash key
		0,                        // reserved
		&this->hHash))            // address of hash object handle
	{
		return 2;
	}

	if (!CryptHashData(
		hHash,                    // handle of the hash object
		this->secret,           // password to hash
		this->secretLength,    // number of bytes of data to add
		0))                       // flags
	{
		return 3;
	}

	if (!CryptDeriveKey(
		this->hProv,                    // handle of the CSP
		CALG_RC4,                 // algorithm ID
		this->hHash,                    // handle to the hash object
		0,                        // flags
		&this->hKey))                   // address of the key handle
	{
		return 4;
	}

	return 0;
}

int HMAC::sign(unsigned char message[], int length)
{
	this->message = new BYTE[length];
	for (int i = 0; i < length; i++)
		this->message[i] = message[i];
	
	/*
		Create an HMAC by performing the following steps:
			1. Call CryptCreateHash to create a hash object and retrieve
			   a handle to it.
			2. Call CryptSetHashParam to set the instance of the HMAC_INFO
			   structure into the hash object.
			3. Call CryptHashData to compute a hash of the message.
			4. Call CryptGetHashParam to retrieve the size, in bytes, of
			   the hash.
			5. Call malloc to allocate memory for the hash.
			6. Call CryptGetHashParam again to retrieve the HMAC hash.
	*/
	if (!CryptCreateHash(
		this->hProv,              // handle of the CSP.
		CALG_HMAC,                // HMAC hash algorithm ID
		this->hKey,               // key for the hash 
		0,                        // reserved
		&this->hHmacHash))        // address of the hash handle
	{
		return 1;
	}

	if (!CryptSetHashParam(
		this->hHmacHash,          // handle of the HMAC hash object
		HP_HMAC_INFO,             // setting an HMAC_INFO object
		(BYTE*)&this->HmacInfo,   // the HMAC_INFO object
		0))                       // reserved
	{
		return 2;
	}

	if (!CryptHashData(
		this->hHmacHash,          // handle of the HMAC hash object
		this->message,            // message to hash
		length,    // number of bytes of data to add
		0))                       // flags
	{
		return 3;
	}

	// Call CryptGetHashParam twice. Call it the first time to retrieve
	// the size, in bytes, of the hash. Allocate memory. Then call 
	// CryptGetHashParam again to retrieve the hash value.

	if (!CryptGetHashParam(
		this->hHmacHash,          // handle of the HMAC hash object
		HP_HASHVAL,               // query on the hash value
		NULL,                     // filled on second call
		&this->dwDataLen,         // length, in bytes, of the hash
		0))
	{
		return 4;
	}

	this->pbHash = (BYTE*)malloc(this->dwDataLen);
	if (NULL == this->pbHash)
	{
		return 5;
	}

	if (!CryptGetHashParam(
		this->hHmacHash,		   // handle of the HMAC hash object
		HP_HASHVAL,                // query on the hash value
		this->pbHash,              // pointer to the HMAC hash value
		&this->dwDataLen,          // length, in bytes, of the hash
		0))
	{
		return 6;
	}

	return 0;
}

PBYTE HMAC::result()
{
	return this->pbHash;
}

DWORD HMAC::length()
{
	return this->dwDataLen;
}

