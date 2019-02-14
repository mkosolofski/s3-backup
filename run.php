<?php
$scannedLogFilePath = '/tmp/S3UploadScanned.txt';
$uploadedLogFilePath = '/tmp/S3UploadUploaded.txt';
$skippedLogFilePath = '/tmp/S3UploadSkipped.txt';
$errorLogFilePath = '/tmp/S3UploadErrors.txt';

$baseDirectory = '/Volumes/Media/Work';
$s3Bucket = '';
$s3Key = '';
$s3Secret ='';

require 'vendor/autoload.php';

use Aws\S3\S3Client;
use Aws\S3\Model\MultipartUpload\UploadBuilder;
use Aws\S3\MultipartUploader;
use Aws\Exception\MultipartUploadException;

$s3Client = new S3Client(
    [
        'region' => 'us-west-2',
        'version' => 'latest',
        'credentials' => [
            'key' => $s3Key,
            'secret' => $s3Secret
        ]
    ]
);


@unlink($scannedLogFilePath);
@unlink($uploadedLogFilePath);
@unlink($skippedLogFilePath);
@unlink($errorLogFilePath);

$pdo = new \PDO('mysql:dbname=media_backup;host=127.0.0.1;', 'script', 'password');

uploadAllFiles();

function uploadAllFiles($scanPath = null)
{
    global $s3Client,
        $s3Bucket,
        $baseDirectory,
        $scannedLogFilePath,
        $uploadedLogFilePath,
        $skippedLogFilePath,
        $errorLogFilePath,
        $baseDirectory,
        $pdo;

    $dateTime = (new \DateTime)->format('y-m-d H:i:s');

    $scanPath = $scanPath ?? $baseDirectory;
    
    foreach(scandir($scanPath) as $file) {
        if ($file == '.' || $file == '..') continue;

        $filePath = $scanPath . '/' . $file;
        if (is_dir($filePath)) {
            uploadAllFiles($filePath);
            continue;
        }

        $objectKey = str_replace($baseDirectory . '/', '', $filePath);
        
        echo PHP_EOL . $objectKey;

        file_put_contents($scannedLogFilePath, $dateTime . ' - ' . $objectKey . PHP_EOL, FILE_APPEND);

        $statement = $pdo->prepare('SELECT file_size FROM `files` WHERE bucket = :bucket AND `file` = :file');
        $statement->execute(['bucket' => $s3Bucket, 'file' => $objectKey]);
        $dbFileSize = $statement->fetchColumn();
        $fileSize = filesize($filePath);
        if ($dbFileSize !== false && $dbFileSize == $fileSize) {
            file_put_contents($skippedLogFilePath, $dateTime . ' - ' . $filePath . PHP_EOL, FILE_APPEND);
            continue;
        }

        try {
            // ~ 10MB
            if ($fileSize > 10000000) {
                try {
                    $uploader = new MultipartUploader(
                        $s3Client,
                        $filePath,
                        [
                            'bucket' => $s3Bucket,
                            'key'    => $objectKey,
                        ]
                    );
                    
                    $uploader->upload();
                } catch (MultipartUploadException $exception) {
                    throw new \RuntimeException($exception->getMessage());
                }

            } else {
                $s3Client->putObject(
                    [
                        'Bucket'     => $s3Bucket,
                        'Key'        => $objectKey,
                        'SourceFile' => $filePath
                    ]
                );

                $s3Client->waitUntil(
                    'ObjectExists',
                    [
                        'Bucket' => $s3Bucket,
                        'Key'    => $objectKey
                    ]
                );            
            }
            
            file_put_contents($uploadedLogFilePath, $dateTime . ' - ' . $filePath . PHP_EOL, FILE_APPEND);
            
            $statement = $pdo->prepare('INSERT INTO `files` (bucket, `file`, file_size) VALUES (:bucket, :file, :fileSize)');
            $statement->execute(['bucket' => $s3Bucket, 'file' => $objectKey, 'fileSize' => $fileSize]);
        } catch (\Exception $e) {
            file_put_contents($errorLogFilePath, $dateTime . ' - ' . $filePath . PHP_EOL . $e->getMessage() . PHP_EOL, FILE_APPEND);
        }
    }
}
