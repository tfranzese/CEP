
import json
import os
import http.client
from multiprocessing import Pool

# Função para consultar informações geográficas usando o serviço dos Correios
def fetch_coordinates_correios(cep):
    conn = http.client.HTTPSConnection("buscacepinter.correios.com.br")
    payload = f'cep={cep}'
    headers = {
       'sec-ch-ua': '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
       'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
       'Cache-Control': 'no-store, no-cache, must-revalidate',
       'DNT': '1',
       'sec-ch-ua-mobile': '?0',
       'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
       'sec-ch-ua-platform': '"macOS"',
       'Accept': '*/*',
       'host': 'buscacepinter.correios.com.br',
       'Cookie': ''
    }
    conn.request("POST", "/app/consulta/html/consulta-detalhes-cep.php", payload, headers)
    res = conn.getresponse()
    data = res.read()
    response_data = json.loads(data.decode("utf-8"))
    
    if not response_data['erro']:
        print(f"Resposta da API para CEP {cep}: {response_data}")
        return response_data, None
    else:
        return None, "Erro na consulta"

# Função que irá processar uma lista de CEPs em paralelo
def process_ceps(ceps):
    cep_info_list = []
    for cep in ceps:
        data, error = fetch_coordinates_correios(cep)
        if data:
            cep_info_list.append(data)
        else:
            print(f"Erro ao consultar o CEP {cep}: {error}")
    return cep_info_list

if __name__ == "__main__":
    # Define o número de processos paralelos
    num_processes = 50

    # Define o diretório do script como o diretório de trabalho atual
    current_directory = os.path.dirname(os.path.realpath(__file__))
    os.chdir(current_directory)

    # Lê o arquivo 'unique_ceps.json' para obter a lista de CEPs
    with open('ceps.json', 'r') as f:
        ceps = json.load(f)

    print("CEPs carregados com sucesso.")

    # Divide a lista de CEPs em lotes de tamanho igual para o multiprocessamento
    batch_size = len(ceps) // num_processes
    ceps_batches = [ceps[i:i+batch_size] for i in range(0, len(ceps), batch_size)]

    # Cria um pool de processos com o número especificado de processos
    with Pool(num_processes) as pool:
        # Mapeia a função de processamento para cada lote de CEPs
        results = pool.map(process_ceps, ceps_batches)

    # Une os resultados de cada lote
    cep_info_list = [info for sublist in results for info in sublist]

    # Salva as informações em um novo arquivo JSON de forma organizada
    with open('ceps.json', 'w') as f:
        json.dump(cep_info_list, f, indent=4)  # Correção aqui: Movido o argumento 'indent' para json.dump()

    print("Informações de CEP salvas com sucesso em formato organizado.")
