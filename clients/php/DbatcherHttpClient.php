<?php

class DbatcherHttpClient
{
    private $curl;
    private $address;

    public function __construct(string $address, int $timeout){
        $this->address = $address;
        $this->curl = curl_init();
        curl_setopt($this->curl, CURLOPT_TIMEOUT, $timeout);
        curl_setopt($this->curl, CURLOPT_HEADER, 0);
        curl_setopt($this->curl, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($this->curl, CURLOPT_ENCODING, "gzip,deflate");
        $headers = [
            "Connection: keep-alive",
            'Keep-Alive: 300'
        ];
        curl_setopt($this->curl, CURLOPT_HTTPHEADER, $headers);

    }

    public function __destruct(){
        curl_close($this->curl);
    }

    public function send(
        string $table, string $fields,
        int $maxRows, int $timeoutMs, bool $sync, bool $persist,
        array $rows, &$error
    ): bool{
        $url = $this->makeUrl($table, $fields, $maxRows, $timeoutMs, $sync, $persist);
        curl_setopt($this->curl, CURLOPT_URL, $url);
        curl_setopt($this->curl, CURLOPT_POSTFIELDS, json_encode($rows));
        $error = curl_exec($this->curl);
        $code = curl_getinfo($this->curl, CURLINFO_RESPONSE_CODE);

        return $code == 200;
    }

    public function makeUrl(
        string $table, string $fields,
        int $maxRows, int $timeoutMs, bool $sync, bool $persist
    ): string{
        $table = urlencode($table);
        $fields = urlencode($fields);
        if ($sync){
            return "{$this->address}/?table=$table&fields=$fields&sync=1";
        }
        $persistStr = "";
        if ($persist)
            $persistStr = "&persist=1";

        return "{$this->address}/?table=$table&fields=$fields&max_rows=$maxRows&timeout_ms=$timeoutMs{$persistStr}";
    }

    public static function sendStatic(
        string $address, int $timeout,
        string $table, string $fields,
        int $maxRows, int $timeoutMs, bool $sync, bool $persist,
        array $rows, &$error
    ): bool{
        $client = new self($address, $timeout);
        return $client->send(
            $table, $fields,
            $maxRows, $timeoutMs, $sync, $persist,
            $rows, $error
        );
    }
}