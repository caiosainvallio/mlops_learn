from minio import Minio
from minio.error import S3Error
import sys
import pandas as pd
import tempfile
import os


def connect_to_minio():
    """
    Conecta com o servidor MinIO usando as credenciais padrão.

    Returns:
        Minio: Cliente MinIO configurado
    """
    # Configurações do MinIO (deve corresponder ao docker-compose.yml)
    endpoint = "localhost:9000"
    access_key = "minioadmin"
    secret_key = "minioadmin123"
    secure = False  # False para HTTP, True para HTTPS

    try:
        # Criar cliente MinIO
        client = Minio(
            endpoint, access_key=access_key, secret_key=secret_key, secure=secure
        )

        print(f"✅ Conectado ao MinIO em {endpoint}")
        return client

    except Exception as e:
        print(f"❌ Erro ao conectar com o MinIO: {e}")
        sys.exit(1)


def list_buckets(client):
    """
    Lista todos os buckets disponíveis no MinIO.

    Args:
        client (Minio): Cliente MinIO configurado
    """
    try:
        buckets = client.list_buckets()

        if buckets:
            print("\n📦 Buckets encontrados:")
            print("-" * 50)
            for bucket in buckets:
                print(f"Nome: {bucket.name}")
                print(f"Data de criação: {bucket.creation_date}")
                print("-" * 50)
        else:
            print("\n📦 Nenhum bucket encontrado.")
            print(
                "💡 Dica: Crie um bucket através do console web em http://localhost:9003/"
            )

    except S3Error as e:
        print(f"❌ Erro S3 ao listar buckets: {e}")
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")


def create_bucket(client, bucket_name="test-bucket"):
    """
    Cria um bucket de exemplo (opcional).

    Args:
        client (Minio): Cliente MinIO configurado
        bucket_name (str): Nome do bucket a ser criado
    """
    try:
        # Verificar se o bucket já existe
        if client.bucket_exists(bucket_name):
            print(f"📦 Bucket '{bucket_name}' já existe.")
            return

        # Criar o bucket
        client.make_bucket(bucket_name)
        print(f"✅ Bucket '{bucket_name}' criado com sucesso!")

    except S3Error as e:
        print(f"❌ Erro S3 ao criar bucket: {e}")
    except Exception as e:
        print(f"❌ Erro inesperado ao criar bucket: {e}")


def delete_bucket(client, bucket_name):
    """
    Exclui um bucket do MinIO.

    Args:
        client (Minio): Cliente MinIO configurado
        bucket_name (str): Nome do bucket a ser excluído
    """
    try:
        # Verificar se o bucket existe
        if not client.bucket_exists(bucket_name):
            print(f"❌ Bucket '{bucket_name}' não existe.")
            return

        # Verificar se o bucket está vazio
        objects = client.list_objects(bucket_name)
        if any(True for _ in objects):
            print(f"⚠️  Bucket '{bucket_name}' não está vazio.")
            response = (
                input("Deseja remover todos os objetos e excluir o bucket? (s/n): ")
                .lower()
                .strip()
            )

            if response not in ["s", "sim", "y", "yes"]:
                print("❌ Operação cancelada.")
                return

            # Remover todos os objetos do bucket
            objects = client.list_objects(bucket_name, recursive=True)
            for obj in objects:
                client.remove_object(bucket_name, obj.object_name)
                print(f"🗑️  Objeto '{obj.object_name}' removido.")

        # Excluir o bucket
        client.remove_bucket(bucket_name)
        print(f"✅ Bucket '{bucket_name}' excluído com sucesso!")

    except S3Error as e:
        print(f"❌ Erro S3 ao excluir bucket: {e}")
    except Exception as e:
        print(f"❌ Erro inesperado ao excluir bucket: {e}")


def upload_file(client, bucket_name, file_path, object_name=None):
    """
    Faz upload de um arquivo para o MinIO.

    Args:
        client (Minio): Cliente MinIO configurado
        bucket_name (str): Nome do bucket de destino
        file_path (str): Caminho do arquivo local
        object_name (str, optional): Nome do objeto no MinIO. Se None, usa o nome do arquivo.
    """
    import os

    try:
        # Verificar se o arquivo existe
        if not os.path.exists(file_path):
            print(f"❌ Arquivo '{file_path}' não encontrado.")
            return

        # Verificar se o bucket existe
        if not client.bucket_exists(bucket_name):
            print(f"❌ Bucket '{bucket_name}' não existe.")
            response = input("Deseja criar o bucket? (s/n): ").lower().strip()

            if response in ["s", "sim", "y", "yes"]:
                client.make_bucket(bucket_name)
                print(f"✅ Bucket '{bucket_name}' criado.")
            else:
                print("❌ Upload cancelado.")
                return

        # Se object_name não foi especificado, usar o nome do arquivo
        if object_name is None:
            object_name = os.path.basename(file_path)

        # Fazer upload do arquivo
        client.fput_object(bucket_name, object_name, file_path)
        print(
            f"✅ Arquivo '{file_path}' enviado como '{object_name}' no bucket '{bucket_name}'!"
        )

        # Mostrar informações do arquivo
        stat = client.stat_object(bucket_name, object_name)
        print(f"📊 Tamanho: {stat.size} bytes")
        print(f"📅 Data de modificação: {stat.last_modified}")

    except S3Error as e:
        print(f"❌ Erro S3 ao fazer upload: {e}")
    except Exception as e:
        print(f"❌ Erro inesperado ao fazer upload: {e}")


def list_objects(client, bucket_name):
    """
    Lista todos os objetos em um bucket.

    Args:
        client (Minio): Cliente MinIO configurado
        bucket_name (str): Nome do bucket
    """
    try:
        # Verificar se o bucket existe
        if not client.bucket_exists(bucket_name):
            print(f"❌ Bucket '{bucket_name}' não existe.")
            return

        # Listar objetos
        objects = client.list_objects(bucket_name, recursive=True)
        object_list = list(objects)

        if object_list:
            print(f"\n📁 Objetos no bucket '{bucket_name}':")
            print("-" * 70)
            for obj in object_list:
                print(f"Nome: {obj.object_name}")
                print(f"Tamanho: {obj.size} bytes")
                print(f"Data de modificação: {obj.last_modified}")
                print("-" * 70)
        else:
            print(f"\n📁 Bucket '{bucket_name}' está vazio.")

    except S3Error as e:
        print(f"❌ Erro S3 ao listar objetos: {e}")
    except Exception as e:
        print(f"❌ Erro inesperado ao listar objetos: {e}")

def download_file(client, bucket_name, object_name, file_path=None):
    """
    Faz download de um arquivo do MinIO.
    
    Args:
        client (Minio): Cliente MinIO configurado
        bucket_name (str): Nome do bucket de origem
        object_name (str): Nome do objeto no MinIO
        file_path (str, optional): Caminho local para salvar. Se None, usa o nome do objeto.
    """
    try:
        # Verificar se o bucket existe
        if not client.bucket_exists(bucket_name):
            print(f"❌ Bucket '{bucket_name}' não existe.")
            return
        
        # Verificar se o objeto existe
        try:
            client.stat_object(bucket_name, object_name)
        except S3Error as e:
            if e.code == 'NoSuchKey':
                print(f"❌ Objeto '{object_name}' não encontrado no bucket '{bucket_name}'.")
                return
            raise
        
        # Se file_path não foi especificado, usar o nome do objeto
        if file_path is None:
            file_path = object_name
        
        # Fazer download do arquivo
        client.fget_object(bucket_name, object_name, file_path)
        print(f"✅ Arquivo '{object_name}' baixado como '{file_path}'!")
        
        # Mostrar informações do arquivo
        import os
        size = os.path.getsize(file_path)
        print(f"📊 Tamanho: {size} bytes")
        
    except S3Error as e:
        print(f"❌ Erro S3 ao fazer download: {e}")
    except Exception as e:
        print(f"❌ Erro inesperado ao fazer download: {e}")


def read_parquet_to_dataframe(client, bucket_name, object_name):
    """
    Lê um arquivo parquet do MinIO e retorna um DataFrame do pandas.
    
    Args:
        client (Minio): Cliente MinIO configurado
        bucket_name (str): Nome do bucket de origem
        object_name (str): Nome do arquivo parquet no MinIO
        
    Returns:
        pandas.DataFrame: DataFrame com os dados do arquivo parquet
    """
    try:
        # Verificar se o bucket existe
        if not client.bucket_exists(bucket_name):
            print(f"❌ Bucket '{bucket_name}' não existe.")
            return None
        
        # Verificar se o objeto existe
        try:
            stat = client.stat_object(bucket_name, object_name)
        except S3Error as e:
            if e.code == 'NoSuchKey':
                print(f"❌ Arquivo '{object_name}' não encontrado no bucket '{bucket_name}'.")
                return None
            raise
        
        # Verificar se o arquivo tem extensão .parquet
        if not object_name.lower().endswith('.parquet'):
            print(f"⚠️  Aviso: O arquivo '{object_name}' não tem extensão .parquet")
        
        # Criar arquivo temporário
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
            temp_path = temp_file.name
        
        try:
            # Fazer download para arquivo temporário
            client.fget_object(bucket_name, object_name, temp_path)
            print(f"📥 Baixando arquivo '{object_name}' temporariamente...")
            
            # Carregar o arquivo parquet como DataFrame
            df = pd.read_parquet(temp_path)
            
            print(f"✅ Arquivo parquet carregado com sucesso!")
            print(f"📊 Shape do DataFrame: {df.shape}")
            print(f"📋 Colunas: {list(df.columns)}")
            print(f"💾 Tamanho do arquivo: {stat.size} bytes")
            
            return df
            
        finally:
            # Remover arquivo temporário
            if os.path.exists(temp_path):
                os.unlink(temp_path)
        
    except S3Error as e:
        print(f"❌ Erro S3 ao ler arquivo parquet: {e}")
    except Exception as e:
        print(f"❌ Erro inesperado ao ler arquivo parquet: {e}")
    
    return None


def upload_dataframe_as_parquet(client, bucket_name, object_name, dataframe):
    """
    Converte um DataFrame do pandas em parquet e faz upload para o MinIO.
    
    Args:
        client (Minio): Cliente MinIO configurado
        bucket_name (str): Nome do bucket de destino
        object_name (str): Nome do arquivo parquet no MinIO
        dataframe (pandas.DataFrame): DataFrame a ser enviado
        
    Returns:
        bool: True se o upload foi bem-sucedido, False caso contrário
    """
    try:
        # Verificar se o DataFrame é válido
        if dataframe is None or dataframe.empty:
            print("❌ DataFrame está vazio ou é None.")
            return False
        
        # Verificar se o bucket existe
        if not client.bucket_exists(bucket_name):
            print(f"❌ Bucket '{bucket_name}' não existe.")
            response = input("Deseja criar o bucket? (s/n): ").lower().strip()

            if response in ["s", "sim", "y", "yes"]:
                client.make_bucket(bucket_name)
                print(f"✅ Bucket '{bucket_name}' criado.")
            else:
                print("❌ Upload cancelado.")
                return False
        
        # Adicionar extensão .parquet se não estiver presente
        if not object_name.lower().endswith('.parquet'):
            object_name += '.parquet'
            print(f"ℹ️  Adicionada extensão .parquet: '{object_name}'")
        
        # Criar arquivo temporário
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
            temp_path = temp_file.name
        
        try:
            # Salvar DataFrame como parquet no arquivo temporário
            print(f"💾 Convertendo DataFrame para parquet...")
            dataframe.to_parquet(temp_path, index=False)
            
            # Verificar tamanho do arquivo gerado
            file_size = os.path.getsize(temp_path)
            print(f"📊 Arquivo parquet criado: {file_size} bytes")
            
            # Fazer upload do arquivo temporário
            client.fput_object(bucket_name, object_name, temp_path)
            print(f"✅ DataFrame enviado como '{object_name}' no bucket '{bucket_name}'!")
            
            # Mostrar informações do DataFrame
            print(f"📋 Shape do DataFrame: {dataframe.shape}")
            print(f"📄 Colunas: {list(dataframe.columns)}")
            print(f"💾 Tamanho do arquivo: {file_size} bytes")
            
            return True
            
        finally:
            # Remover arquivo temporário
            if os.path.exists(temp_path):
                os.unlink(temp_path)
        
    except S3Error as e:
        print(f"❌ Erro S3 ao fazer upload do DataFrame: {e}")
    except Exception as e:
        print(f"❌ Erro inesperado ao fazer upload do DataFrame: {e}")
    
    return False