/*
 * Copyright 2019 Koverse, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.koverse.examples.integration;

import com.koverse.com.google.common.io.BaseEncoding;
import com.koverse.sdk.annotation.Description;
import com.koverse.sdk.annotation.StringParameter;
import com.koverse.sdk.record.FieldMasker;
import com.koverse.sdk.record.FieldUnmaskable;

import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

@Description(
    id = "example-symmetric-encrypt-value-masker",
    name = "Example Symmetric AES Encryption Masker",
    description = "Encrypt values using a symmetric key using AES",
    majorVersion = 1,
    minorVersion = 0,
    patchVersion = 0)
public class SymmetricEncryptMasker implements FieldMasker, FieldUnmaskable {

  private static final Charset UTF8 = Charset.forName("UTF-8");

  // set scope to this for testability
  @StringParameter(
      id = "key",
      name = "The secret key",
      groupName = "",
      required = true,
      description = "Key to use to encrypt and decrypt values",
      hideInput = true,
      defaultValue = "")
  String key;

  private Cipher encryptionCipher = null;
  private Cipher decryptionCipher = null;

  // generate this once so we amortize creation cost, which may be high, over many values
  private synchronized Cipher getEncryptionCipher() throws
      NoSuchPaddingException,
      NoSuchAlgorithmException,
      InvalidKeyException {

    if (encryptionCipher == null) {
      encryptionCipher = createCipher(Cipher.ENCRYPT_MODE);
    }

    return encryptionCipher;
  }

  // generate this once so we amortize creation cost, which may be high, over many values
  private synchronized Cipher getDecryptionCipher() throws
      NoSuchPaddingException,
      NoSuchAlgorithmException,
      InvalidKeyException {

    if (decryptionCipher == null) {
      decryptionCipher = createCipher(Cipher.DECRYPT_MODE);
    }

    return decryptionCipher;
  }

  // generate this once so we amortize creation cost, which may be high, over many values
  private Cipher createCipher(int mode)
      throws NoSuchAlgorithmException, InvalidKeyException, NoSuchPaddingException {

    final byte[] keyBytes = key.getBytes(UTF8);
    final MessageDigest sha = MessageDigest.getInstance("SHA-256");
    final byte[] shaKeyBytes = sha.digest(keyBytes);
    final byte[] aesKeyBytes = Arrays.copyOf(shaKeyBytes, 16);
    final SecretKeySpec secretKeySpec = new SecretKeySpec(aesKeyBytes, "AES");
    final Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");

    cipher.init(mode, secretKeySpec);

    return cipher;
  }

  @Override
  public Object mask(Object o) {
    try {
      final byte[] messageBytes = o.toString().getBytes(UTF8);
      final byte[] encryptedBytes = getEncryptionCipher().doFinal(messageBytes);

      return BaseEncoding.base16().encode(encryptedBytes);

    } catch (NoSuchPaddingException
        | InvalidKeyException
        | NoSuchAlgorithmException
        | IllegalBlockSizeException
        | BadPaddingException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Object unmask(Object o) {
    try {
      final byte[] messageBytes = BaseEncoding.base16().decode(o.toString());
      final byte[] decryptedBytes = getDecryptionCipher().doFinal(messageBytes);

      return new String(decryptedBytes, UTF8);

    } catch (NoSuchPaddingException
        | InvalidKeyException
        | NoSuchAlgorithmException
        | IllegalBlockSizeException
        | BadPaddingException ex) {
      throw new RuntimeException(ex);
    }
  }

}
