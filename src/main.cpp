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
#include "HMACFilter.h"

#include <iostream>
using namespace std;

int main()
{

    int dataLen = 3;
    int keyLen = 3;

    char data[dataLen] = { 0x1, 0x2, 0x3};
    char data2[dataLen] = { 0x3, 0x4, 0x5};
    char key[keyLen] = { 0x3, 0x4, 0x5};

    HMAC *hmac = new HMAC(key, keyLen);
    char *md5str = hmac->sign(data, dataLen);

    HMACFilter *hmacFilter = new HMACFilter(hmac);

    cout << hmacFilter->contains(data, dataLen) << endl;
    hmacFilter->insert(data, 3);
    cout << hmacFilter->contains(data, dataLen) << endl;

    unsigned char* canMsg = hmacFilter->getCanMessage();

    for(int i = 0; i < BYTE_S; i++)
        printf("%x ", canMsg[i]);

    printf("\n");

    cout << hmacFilter->contains(data2, dataLen) << endl;
    hmacFilter->insert(data2, dataLen);
    cout << hmacFilter->contains(data2, dataLen) << endl;

    cout << hmacFilter->getElemCount() << endl; 
    unsigned char* canMsg2 = hmacFilter->getCanMessage();

    for(int i = 0; i < BYTE_S; i++)
        printf("%x ", canMsg2[i]);


    printf("\n");
    return 0;
}