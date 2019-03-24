# finish_mulit_part_upload

Boto3 has an S3Transfer module that allows for a quick and easy way to upload files to your S3 bucket. If the file to upload is greater than a certain threshold, it will automatically switch to multi-part uploads (mpus). 

While this is a useful feature, it makes it difficult to complete the mpu if it was interrupted in the middle. There is no straightforward way to see what the UploadId is and finish uploads using the S3Transfer. Instead, you have to manually use boto3's multipart_upload. 

This script can be used to complete mpus without knowing what the UploadId is. All that is needed is the bucket where files were uploading, a prefix, if it exists, and the directory where the files are saved locally. The script will check to see if any mpus exist. If there are more than one, it keeps the first and aborts the remaining hanging mpu. It will then check to see the files to upload are located in the specified directory.

For each mpu to complete, the script streams the data by chunks. If the part was previously uploaded, it compares the md5 to the ETag stored in AWS to confirm the file was not corrupted or modified in the time in between. Otherwise, it uploads the part to AWS. Once it has reached the end of the file, it completes the upload. 
