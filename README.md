# BigData Impementasi-Spark

## Pengertian Apache Spark
Apache Spark adalah kerangka kerja pemrosesan paralel sumber terbuka yang mendukung pemrosesan dalam memori untuk meningkatkan performa aplikasi yang menganalisis big data. Solusi big data dirancang untuk menangani data yang terlalu besar atau kompleks untuk database tradisional. Spark memproses sejumlah besar data dalam memori, yang jauh lebih cepat daripada alternatif berbasis disk. 
Apache Spark (Spark) adalah mesin pemrosesan data sumber terbuka untuk kumpulan data yang besar. Spark dirancang untuk memberikan kecepatan komputasi, skalabilitas, dan kemampuan pemrograman yang diperlukan untuk Big Data - khususnya untuk streaming data, data grafik, pembelajaran mesin, dan aplikasi kecerdasan buatan (AI).

**Kode Program tersebut implementasi Apache Spark dengan libarary Spark MlLib dan Spark SQL dengan menggunakan bahasa pemrograman yaitu python**

## Arsitektur Apache Spark
Apache Spark memiliki tiga komponen utama: driver, eksekutor, dan manajer kluster. Aplikasi Spark berjalan sebagai serangkaian proses independen pada kluster, yang dikoordinasikan oleh program driver.
Apache Spark bekerja dalam arsitektur master-slave di mana master disebut "Driver" dan slave disebut "Workers". Ketika menjalankan aplikasi Spark, Spark Driver membuat konteks yang merupakan titik masuk ke aplikasi, dan semua operasi (transformasi dan tindakan) dijalankan pada node pekerja, dan sumber daya dikelola oleh Cluster Manager.
<p align="center">
  <img align="middle" alt="Arsitektur" width="50%" src="https://github.com/sitiaisyah14/BigData_Impementasi-Spark/blob/main/image/arsitektur.png" />
</p>

&nbsp;

## Library Perangkat Lunak Tambahan Apache Spark  
Spark memiliki berbagai pustaka yang memperluas kemampuan untuk pembelajaran mesin, kecerdasan buatan (AI), dan pemrosesan streaming. Contoh apabila Spark yang dapat dijalankan dengan Machine Learning, kecerdasan buatan (AI), dan pemrosesan streaming, antara lain 
<p align="center">
  <img align="middle" alt="Arsitektur" width="50%" src="https://github.com/sitiaisyah14/BigData_Impementasi-Spark/blob/main/image/macamspark.png" />
</p>
&nbsp;

### 1. Spark MlLib
   
Dengan menggunakan Apache Spark MLlib, dapat melakukan berbagai tugas machine learning seperti regresi, klasifikasi, klastering, pengolahan data, ekstraksi fitur, evaluasi model, dan banyak lagi. Library ini dirancang untuk bekerja secara efisien dengan data dalam skala besar dan berintegrasi dengan fitur-fitur Apache Spark seperti pemrosesan paralel dan distribusi.

Apache Spark MLlib mendukung berbagai algoritma machine learning, termasuk regresi linier, regresi logistik, decision tree, random forest, k-means, collaborative filtering, dan masih banyak lagi. Selain itu, MLlib juga menyediakan fitur-fitur seperti preproses data, pemilihan fitur, evaluasi model, dan pipelining untuk mempermudah pengembangan dan penerapan model machine learning.

Secara keseluruhan, Apache Spark MLlib memungkinkan untuk memanfaatkan kekuatan Apache Spark dalam pemrosesan big data untuk membangun dan menerapkan model machine learning yang kuat dan efisien.
### 2. Spark GraphX

Spark GraphX adalah komponen dari Apache Spark yang dirancang khusus untuk pemrosesan dan analisis data berstruktur grafik. Graf adalah representasi visual atau matematis dari hubungan antara entitas yang berbeda. Dalam konteks Spark GraphX, grafik terdiri dari kumpulan simpul (nodes) yang mewakili entitas dan kumpulan tepi (edges) yang mewakili hubungan antara simpul-simpul tersebut.

Spark GraphX menyediakan API yang kuat untuk memanipulasi dan menganalisis data berstruktur grafik secara efisien dalam skala besar. Beberapa fitur dan fungsionalitas yang disediakan oleh Spark GraphX antara lain:

1. Representasi Data Grafik: Spark GraphX menyediakan struktur data yang efisien untuk merepresentasikan data berstruktur grafik, yang memungkinkan manipulasi dan analisis yang efisien dari grafik yang besar.

2. Operasi Grafik: Spark GraphX mendukung berbagai operasi grafik yang umum, seperti pencarian jalur terpendek, perhitungan PageRank, klasterisasi grafik, dan banyak lagi. Dapat menggunakan API Spark GraphX untuk melakukan operasi ini secara efisien pada grafik yang besar.

3. Transformasi dan Pemrosesan Grafik: Spark GraphX menyediakan fungsi-fungsi untuk melakukan transformasi pada grafik, seperti pemetaan (mapping) simpul atau tepi, filter, penggabungan, dan transformasi lainnya. Hal ini memungkinkan untuk melakukan manipulasi data grafik dengan mudah.

4. Integrasi dengan Komponen Spark Lainnya: Spark GraphX terintegrasi dengan baik dengan komponen lain dalam ekosistem Apache Spark. Dapat memanfaatkan kekuatan pemrosesan paralel Spark untuk melakukan analisis grafik yang skalabel dan cepat.

Dengan Spark GraphX, Dapat memanfaatkan kemampuan Apache Spark untuk pemrosesan big data dalam konteks data berstruktur grafik. Ini memungkinkan untuk melakukan analisis yang kompleks, seperti analisis jejaring sosial, analisis jaringan transportasi, analisis keamanan, dan banyak lagi, dengan memanfaatkan kekuatan pemrosesan distribusi dan skalabilitas Spark.

### 3. Spark Streaming

Spark Streaming adalah komponen dari Apache Spark yang dirancang untuk memproses dan menganalisis data secara real-time. Dengan menggunakan Spark Streaming, dapat melakukan pemrosesan dan analisis data streaming secara paralel dan distribusi dalam waktu nyata.

Spark Streaming mengadopsi model pemrograman yang sama dengan Apache Spark, yang berbasis pada konsep Resilient Distributed Datasets (RDDs). Namun, Spark Streaming memperluas kemampuan Spark dengan menambahkan abstraksi tambahan yang disebut Discretized Streams (DStreams). DStreams adalah urutan terdistribusi dari RDDs yang direpresentasikan dalam bentuk waktu diskret.

Beberapa fitur dan fungsionalitas Spark Streaming termasuk:

1. Penerimaan Data Streaming: Spark Streaming mendukung penerimaan data streaming dari berbagai sumber seperti Kafka, Flume, Kinesis, socket TCP/IP, dan masih banyak lagi. Hal ini memungkinkan  untuk mengintegrasikan dengan sistem streaming yang ada dan memproses data secara real-time.

2. Transformasi Data Streaming: Spark Streaming menyediakan operasi transformasi tingkat tinggi yang dapat diterapkan pada DStreams, seperti pemetaan (mapping), filter, agregasi, join, dan lainnya. Dapat menggunakan operasi ini untuk memanipulasi dan menganalisis data streaming dengan cepat dan efisien.

3. Integrasi dengan Ekosistem Spark: Spark Streaming terintegrasi dengan baik dengan komponen lain dalam ekosistem Apache Spark. Dapat memanfaatkan kekuatan pemrosesan paralel Spark untuk melakukan pemrosesan data streaming secara efisien dan dengan skala yang besar.

4. Fault-tolerant dan Exactly-once Semantics: Spark Streaming menyediakan toleransi kesalahan dan jaminan exactly-once semantics. Ini berarti bahwa jika terjadi kegagalan di dalam sistem, Spark Streaming dapat mengatasi dan memulihkan secara otomatis tanpa kehilangan data dan menghasilkan output yang konsisten.

Dengan Spark Streaming, dapat memanfaatkan kekuatan Apache Spark untuk menganalisis dan memproses data secara real-time. Ini memungkinkan untuk melakukan tugas-tugas seperti analisis data streaming, deteksi anomali real-time, pemantauan sistem secara real-time, dan aplikasi real-time lainnya dengan skala yang besar dan kemampuan pemrosesan paralel yang tinggi.


### 4. Spark SQL
Spark SQL adalah modul dari Apache Spark yang menyediakan antarmuka untuk pemrosesan data terstruktur dan semi-terstruktur. Modul ini memungkinkan  untuk melakukan query dan analisis data menggunakan bahasa SQL (Structured Query Language) pada data yang terstruktur, seperti tabel relasional dalam basis data atau file CSV, JSON, Parquet, Avro, dan format data lainnya.

Dalam Spark SQL, data terstruktur direpresentasikan sebagai DataFrames atau Datasets. DataFrames adalah koleksi terdistribusi dari baris yang diorganisir ke dalam kolom yang dinamis, mirip dengan tabel dalam database relasional. Datasets adalah versi tipe terkuat dari DataFrames yang memanfaatkan fitur-fitur bahasa pemrograman seperti tipe statis dan lambda functions.

Berikut adalah beberapa fitur dan fungsionalitas Spark SQL:

1. Query dan Analisis Data: Spark SQL memungkinkan untuk melakukan query dan analisis data menggunakan perintah SQL.  Spark SQL dapat melakukan operasi seperti seleksi, proyeksi, join, agregasi, dan lainnya pada DataFrames atau Datasets menggunakan sintaks SQL yang familier.

2. Integrasi dengan Sumber Data: Spark SQL dapat membaca dan menulis data dari berbagai sumber seperti Hive, Avro, Parquet, JSON, JDBC, dan banyak lagi. Ini memungkinkan untuk memanfaatkan data dari berbagai sistem penyimpanan dan format data.

3. Pengoptimalan Otomatis: Spark SQL menggunakan Catalyst Optimizer untuk melakukan optimasi query secara otomatis. Optimizer ini dapat mengubah query menjadi bentuk yang lebih efisien dan menggunakan teknik-teknik seperti prediksi kolom, penghapusan kode yang tidak digunakan, dan pemrosesan kolom secara vektorisasi.

4. Integrasi dengan Ekosistem Spark: Spark SQL terintegrasi dengan baik dengan komponen lain dalam ekosistem Apache Spark. Dapat menggunakan Spark SQL bersama dengan komponen seperti Spark Streaming, MLlib, dan GraphX untuk melakukan analisis data terstruktur dan semi-terstruktur secara komprehensif.

Dengan Spark SQL, dapat menggabungkan kekuatan Apache Spark dalam pemrosesan data besar-besaran dengan fleksibilitas dan ekspresivitas bahasa SQL untuk menganalisis data terstruktur dan semi-terstruktur. Ini memungkinkan untuk melakukan query dan analisis data yang kompleks secara efisien dan skalabel dalam lingkungan Apache Spark.





