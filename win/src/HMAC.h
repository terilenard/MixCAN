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
* Part of the source code was taken from: https://docs.microsoft.com/en-us/windows/win32/seccrypto/example-c-program--creating-an-hmac
*/

#pragma once
#ifndef INCLUDE_HMAC_H
#define INCLUDE_HMAC_H
#include <cstdlib>
#include <windows.h>
#include <wincrypt.h>


class HMAC
{

public:
	HMAC(unsigned char secret[], int length);
	~HMAC();

	int setup();
	int sign(unsigned char message[], int length);
	PBYTE result();
	DWORD length();

	// Pointer to the hash.
	PBYTE       pbHash = NULL;

protected:
	// Handle to a cryptographic service provider (CSP)
	HCRYPTPROV  hProv = NULL;

	// Handle to the hash object needed to create a hash.
	HCRYPTHASH  hHash = NULL;

	//Handle to a symmetric key. This example creates a key for the RC4 algorithm.
	HCRYPTKEY   hKey = NULL;

	//Handle to an HMAC hash.
	HCRYPTHASH  hHmacHash = NULL;

	// Length, in bytes, of the hash.
	DWORD       dwDataLen = 0;

	// Password string used to create a symmetric key.
	BYTE *secret;

	// Message string to be hashed.
	BYTE *message;

	// Instance of an HMAC_INFO structure that contains
	// information about the HMAC hash.
	HMAC_INFO   HmacInfo;

	int secretLength;
};
#endif
