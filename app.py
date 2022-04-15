## IMPORTS
from google.cloud import firestore
from google.cloud.firestore_v1.query import Query
import json
import random
from datetime import datetime, timedelta
import pytz
import streamlit as st
st.set_page_config(layout="wide")



## DB CONNECT
try:
    key_dict = json.loads(st.secrets["textkey"])
    db = firestore.Client.from_service_account_info(key_dict)
except Exception as e:
    st.write(f":warning: Erro na conexão com o Banco de Dados:\n{e}")
    st.write("Se persistir o erro, contate o desenvolvedor!")





## FUNÇÕES
def __spaces(repeticoes: int = 3) -> None:
    """Escreve espaços vazios para como forma de adicionar espaços verticais entre os elementos.

    Args:
        repeticoes (int, optional): Quantidade de espaços verticais. Defaults to 3.
    """
    _ = [st.write("") for _ in range(repeticoes)]





def __parse_to_boolean(submit_args: dict) -> dict:
    """Converte os estados dos radio buttons de Sim ou Não para booleano.

    Args:
        submit_args (dict): Argumentos para subir para o Banco de Dados. 

    Returns:
        dict: Arguntmentos devidamente convertidos. 
    """
    new_args = submit_args

    for key_out in new_args.keys():
        for key_in in new_args[key_out].keys():
            if new_args[key_out][key_in] == "Não":
                new_args[key_out][key_in] = False
            elif new_args[key_out][key_in] == "Sim":
                new_args[key_out][key_in] = True        

    return new_args





def __query(area: str, home: bool=False) -> Query.stream:
    """Busca no Banco de Dados com filtros.

    Args:
        area (str): Identificador da coleção no banco de dados ('eta', 'etei', ou 'obs').
        home (bool, optional): Opção para identificar a busca específica para a Home. Defaults to False.

    Returns:
        Query.stream: Generator com objetos correspondentes aos filtros de busca aplicados.
    """
    if home:
        fechamentos_ref = db.collection(f"fechamento_{area}")
        docs = fechamentos_ref.order_by(
            u"date", direction=firestore.Query.DESCENDING
        ).limit(1)

        date_query = datetime.strptime(
            f"{docs.get()[0].to_dict()['date'].astimezone(pytz.timezone('America/Sao_Paulo')).date()}", "%Y-%m-%d"
        )
        shift = docs.get()[0].to_dict()["endedshift"]

    else:
        date_query = datetime.strptime(f"{st.session_state.date_search}", "%Y-%m-%d")
        shift = st.session_state.sft_search


    fechamentos_ref = db.collection(f"fechamento_{area}")
    doc_ref = fechamentos_ref.where(
        u"date", u">", date_query
    ).where(
        u"date", u"<", date_query + timedelta(days=1)
    ).where(
        u"endedshift", u"==", f"{shift}"
    ).order_by(
        u"date", direction=firestore.Query.ASCENDING
    )

    return doc_ref.stream()





def __merge_docs(area: str, query: Query.stream) -> dict:
    """Concatena os documentos com a mesma especificação em um só.

    Args:
        area (str): Identificador da coleção no banco de dados ('eta', 'etei', ou 'obs').
        query (Query.stream): Stream de queries resultantes da busca filtrada no banco de dados

    Returns:
        dict: Objeto com as informações concatenadas. 
        Retorna None caso não haja registros para a busca aplicada.
    """    
    with open("db_fields.json", mode="r") as file:
        fields_json = json.load(file)
        merged = dict.fromkeys(fields_json[area])

    for doc in query:
        for key in merged.keys():
            if key == "date":
                if doc.to_dict()[key] == "":
                    return None
                merged[key] = doc.to_dict()[key].astimezone(pytz.timezone("America/Sao_Paulo"))
            
            elif (key == "endedshift") | (merged[key] == ""):
                merged[key] = doc.to_dict()[key]   
            
            else:
                merged[key] = doc.to_dict()[key]

    return merged





def __display_shift_info(query_eta: dict, query_etei: dict, query_obs: dict) -> None:
    """Apresenta as informações de um turno na aplicação.

    Args:
        query_eta (dict): Objeto resultante da busca no BD na coleção de fechamentos da ETA com os valores a serem apresentados na aplicação.
        query_etei (dict): Objeto resultante da busca no BD na coleção de fechamentos da ETEI com os valores a serem apresentados na aplicação.
        query_obs (dict): Objeto resultante da busca no BD na coleção de fechamentos das observações com os valores a serem apresentados na aplicação.
    """
    # Verifica a ausência de registro
    if (query_eta == None) | (query_eta["date"] == ""):
        st.write("## :warning: Busca não encontrada!")

    else:
        try:
            # dia, hora e turno da última modificação
            col_mod, col_data, col_hora, col_turno, col_spare = st.columns([2, 1, 1, 1, 3])

            with col_mod:
                st.subheader("Última Modificação:\n\n")        
            with col_data:
                __spaces(1)
                st.write(f"__Dia: {query_eta['date'].astimezone(pytz.timezone('America/Sao_Paulo')).strftime('%d/%m/%Y')}__")
            with col_hora:
                __spaces(1)
                st.write(f"__Hora: {query_eta['date'].astimezone(pytz.timezone('America/Sao_Paulo')).strftime('%H:%M:%S')}__")
            with col_turno:
                __spaces(1)
                st.write(f"__Turno: {query_eta['endedshift']}__")
            with col_spare:
                st.empty()

            # Detalhes do turno selecinado
            col_eta, col_spare, col_etei, col_spare_2, col_obs = st.columns([4,1,4,1,5])

            with col_eta:
                __spaces(1)
                st.write("## ETA")
                # Itens
                __spaces(1)
                if query_eta["troca_filtro_polidor_1"]:
                    st.success("[Sim] Troca Filtro Polidor 1")      # Sim: verde
                else:
                    st.error("[Não] Troca Filtro Polidor 1")        # Não: vermelho

                __spaces(1)
                if query_eta["troca_filtro_polidor_2"]:
                    st.success("[Sim] Troca Filtro Polidor 2")      # Sim: verde
                else:
                    st.error("[Não] Troca Filtro Polidor 2")        # Não: vermelho

                __spaces(1)
                if query_eta["coluna_di_saturada_100"]:
                    st.error("[Sim] Coluna DI Saturada 100")        # Sim: vermelho
                else:
                    st.success("[Não] Coluna DI Saturada 100")      # Não: verde

                __spaces(1)
                if query_eta["coluna_di_saturada_101"]:
                    st.error("[Sim] Coluna DI Saturada 101")        # Sim: vermelho
                else:
                    st.success("[Não] Coluna DI Saturada 101")      # Não: verde

                __spaces(1)
                if query_eta["regenerar_100"]:
                    st.error("[Sim] Necessário Regenerar 100")      # Sim: vermelho
                else:
                    st.success("[Não] Necessário Regenerar 100")    # Não: verde

                __spaces(1)
                if query_eta["regenerar_101"]:
                    st.error("[Sim] Necessário Regenerar 101")      # Sim: vermelho
                else:
                    st.success("[Não] Necessário Regenerar 101")    # Não: verde
                


            with col_spare:
                st.empty()


            with col_etei:
                __spaces(1)
                st.write("## ETEI")
                # Itens
                __spaces(1)
                if query_etei["troca_filtro_polidor"]:
                    st.success("[Sim] Troca do Filtro Polidor")     # Sim: verde
                else:
                    st.error("[Não] Troca do Filtro Polidor")       # Não: vermelho

                __spaces(1)
                if query_etei["dosou_antiespumante_mbr"]:
                    st.error("[Sim] Dosou Antiespumante (MBR)")     # Sim: vermelho
                else:
                    st.success("[Não] Dosou Antiespumante (MBR)")   # Não: verde

                __spaces(1)
                if query_etei["envio_sanitario_mbr"]:
                    st.success("[Sim] Envio Sanitária (MBR)")       # Sim: verde
                else:
                    st.error("[Não] Envio Sanitária (MBR)")         # Não: vermelho

                __spaces(1)
                if query_etei["transbordou_mbr"]:
                    st.error("[Sim] Transbordo (MBR)")              # Sim: vermelho
                else:
                    st.success("[Não] Transbordo (MBR)")            # Não: verde

                __spaces(1)
                if query_etei["quebra_emulsao"]:
                    st.success("[Sim] Quebra de Emulsão")           # Sim: verde
                else:
                    st.error("[Não] Quebra de Emulsão")             # Não: vermelho

                __spaces(1)
                if query_etei["nivel_silo_cal"] < 20:
                    st.error(f"Nível do Silo de Cal: {query_etei['nivel_silo_cal']}%")      # < 20%: vermelho
                else:
                    st.success(f"Nível do Silo de Cal: {query_etei['nivel_silo_cal']}%")    # < 20%: verde


            with col_spare_2:
                st.empty()


            with col_obs:
                __spaces(1)
                st.write("## Observações")

                if ((query_obs["geral"] == "") and (query_obs["eta_etei"] == "") and (query_obs["quimicos"] == "")
                and (query_obs["mbr_aeracao_sanitaria"] == "") and (query_obs["utilidades"] == "") and (query_obs["scrap_bulk"] == "")):
                    st.write("Sem observações.")

                else:
                    if query_obs["geral"] != "":
                        st.write("#### Gerais:")
                        st.write(f'> {query_obs["geral"]}')
                        __spaces(1)

                    if query_obs["eta_etei"] != "":
                        st.write("#### ETA / ETEI:")
                        st.write(f'> {query_obs["eta_etei"]}')
                        __spaces(1)
                        
                    if query_obs["quimicos"] != "":
                        st.write("#### Químicos:")
                        st.write(f'> {query_obs["quimicos"]}')
                        __spaces(1)

                    if query_obs["mbr_aeracao_sanitaria"] != "":
                        st.write("#### MBR / Aeração / Sanitária:")
                        st.write(f'> {query_obs["mbr_aeracao_sanitaria"]}')
                        __spaces(1)

                    if query_obs["utilidades"] != "":
                        st.write("#### Utilidades:")
                        st.write(f'> {query_obs["utilidades"]}')
                        __spaces(1)

                    if query_obs["scrap_bulk"] != "":
                        st.write("#### Scrap / Bulk Systems:")
                        st.write(f'> {query_obs["scrap_bulk"]}')
                        __spaces(1)
                
        except AttributeError as e:
            st.write(":warning: Busca não encontrada.")





def _upload_shift_data(submit_args: dict) -> None:
    """Faz o upload de dados do turno selecionado para o banco de dados.

    Args:
        submit_args (dict): Objeto com os valores a serem salvos.
    """
    submit_args = __parse_to_boolean(submit_args=submit_args)
    now = datetime.now().astimezone(pytz.timezone("America/Sao_Paulo"))

    new_id = now.strftime("%d%m%Y") + st.session_state.sft + str(random.randrange(1, 10**3)).zfill(4)  
    for key in submit_args.keys():
        submit_args[key]["date"] = now

    # Envia para BD
    doc_ref_eta = db.collection(u"fechamento_eta").document(new_id)
    doc_ref_eta.set(submit_args["ETA"])

    doc_ref_etei = db.collection(u"fechamento_etei").document(new_id)
    doc_ref_etei.set(submit_args["ETEI"])

    doc_ref_obs = db.collection(u"fechamento_obs").document(new_id)
    doc_ref_obs.set(submit_args["OBS"])





def __conflito_checkboxes() -> bool:
    """Verifica se há conflito nas marcações dos checkboxes.

    Returns:
        bool: True se houver algum conflito. False se estiver tudo ok.
    """
    for area in ["eta", "etei"]:
        with open("db_fields.json", mode="r") as file:
            fields_json = json.load(file)
            keys = dict.fromkeys(fields_json[area])

        for key in keys:
            if key not in ["id", "date", "endedshift", "silo_cal"]:
                if st.session_state[f"{key}_sim"] and st.session_state[f"{key}_nao"]:
                    return True 
                elif not st.session_state[f"{key}_sim"] and not st.session_state[f"{key}_nao"]:
                    return True 
                
        return False





def __parse_str_to_float(string: str) -> float:
    return float(string.replace(',', '.').replace('%', ''))
    




def __submit_callback() -> None:
    """Callback Function para o Submit do Forms para Inserir Dados.
    """
    if st.session_state.id == "":
        st.error("Identificação não informada!")
    elif st.session_state.sft == "Selecione":
        st.error("Turno não selecionado!")
    elif st.session_state.silo_cal == "":
        st.error("Nível de Silo de Cal não informado!")
    elif __conflito_checkboxes():
        st.error("Marcação conflitante!")

    else:   
        submit_args = {
            "ETA": {
                "id": st.session_state.id,
                "endedshift": st.session_state.sft,
                "coluna_di_saturada_100": st.session_state.coluna_di_saturada_100_sim,
                "coluna_di_saturada_101": st.session_state.coluna_di_saturada_101_sim,
                "regenerar_100": st.session_state.regenerar_100_sim,
                "regenerar_101": st.session_state.regenerar_101_sim,
                "troca_filtro_polidor_1": st.session_state.troca_filtro_polidor_1_sim,
                "troca_filtro_polidor_2": st.session_state.troca_filtro_polidor_2_sim,
            },
            "ETEI": {
                "id": st.session_state.id,
                "endedshift": st.session_state.sft,
                "dosou_antiespumante_mbr": st.session_state.dosou_antiespumante_mbr_sim,
                "envio_sanitario_mbr": st.session_state.envio_sanitario_mbr_sim,
                "transbordou_mbr": st.session_state.transbordou_mbr_sim,
                "troca_filtro_polidor": st.session_state.troca_filtro_polidor_sim,
                "quebra_emulsao": st.session_state.quebra_emulsao_sim,
                "nivel_silo_cal": __parse_str_to_float(st.session_state.silo_cal),
            },
            "OBS": {
                "id": st.session_state.id,
                "endedshift": st.session_state.sft,
                "geral": st.session_state.obs_geral,
                "eta_etei": st.session_state.obs_eta_etei,
                "quimicos": st.session_state.obs_quim,
                "mbr_aeracao_sanitaria": st.session_state.obs_mbr_aeracao_sanitaria,
                "utilidades": st.session_state.obs_utilidades,
                "scrap_bulk": st.session_state.obs_scrap_bulk
            }
        }

        
        # Sobe os dados do turno para o banco de dados
        _upload_shift_data(submit_args)

        # Mensagem de sucesso
        st.success("Dados enviados com sucesso!")
        
        # Limpar os campos de se inserir dados
        clear_list_1 = ["id", "sft", "silo_cal", "obs_geral", "obs_eta_etei", 
        "obs_quim", "obs_mbr_aeracao_sanitaria", "obs_utilidades", "obs_scrap_bulk"]
        clear_list_2 = ["coluna_di_saturada_100", "coluna_di_saturada_101", "regenerar_100",
        "regenerar_101", "troca_filtro_polidor_1", "troca_filtro_polidor_2", "dosou_antiespumante_mbr",
        "envio_sanitario_mbr", "transbordou_mbr", "troca_filtro_polidor", "quebra_emulsao"]

        for key in clear_list_1:
            if key not in st.session_state.keys():
                st.error("Chave inexistente")
            elif key == "sft":
                st.session_state[key] = "Selecione"
            else:
                st.session_state[key] = ""

        for key in clear_list_2:
            if f"{key}_sim" not in st.session_state.keys():
                st.error("Erro ao limpar formulário: chave inexistente.")
            else:
                st.session_state[f"{key}_sim"] = False
                st.session_state[f"{key}_nao"] = False





def __search_callback(home: bool=False) -> None:
    """Callback Function para Buscar dados e apresentá-los na aplicação.
    Serve tanto para a Home quanto para a tela de Buscar.

    Args:
        home (bool, optional): Opção para identificar a busca específica para a Home. Defaults to False.
    """
    # Busca todos os dados da data e turno escolhidos
    try:
        query_eta = __query(area="eta", home=home)
        query_etei = __query(area="etei", home=home)
        query_obs = __query(area="obs", home=home)

        # junta todas as informações
        merged_query_eta = __merge_docs(area="eta", query=query_eta)
        merged_query_etei = __merge_docs(area="etei", query=query_etei)
        merged_query_obs = __merge_docs(area="obs", query=query_obs)

        # mostra as informações na tela
        __display_shift_info(merged_query_eta, merged_query_etei, merged_query_obs)
    
    except IndexError:
        st.write(":warning: Erro ao buscar no Banco de Dados. Tente novamente.")

        return None


    


def __inserir_dados() -> None:
    """Estrutura de Formulário para inserir novos dados de turno para o Banco de Dados.
    """
    # Header
    st.header("Inserir Dados")

    col_id, col_shift, col_empty  = st.columns([1,1,5])
    with col_id:
        st.text_input(label="ID", placeholder="Insira seu ID", key="id")
    with col_shift:
        st.selectbox(label="Turno: ", options=["Selecione", "A", "B", "C"], key="sft")
    with col_empty:
        st.empty()    
    
    # Forms
    with st.form(key='form_in', clear_on_submit=False):
        col_eta, col_eta_check_sim, col_eta_check_nao, col_empty1, col_etei, col_etei_check_sim, col_etei_check_nao = st.columns([2,1,1,1,2,1,1])

        # Coluna da ETA
        with col_eta:
            st.header("ETA")
            __spaces(2)
            st.write('<p style="font-family:Roboto:wght@100; letter-spacing:.7px; font-size: 20px;">Troca do Filtro Polidor 1</p>', unsafe_allow_html=True)
            __spaces(2)
            st.write('<p style="font-family:Roboto:wght@100; letter-spacing:.7px; font-size: 20px;">Troca do Filtro Polidor 2</p>', unsafe_allow_html=True)  
            __spaces(2)
            st.write('<p style="font-family:Roboto:wght@100; letter-spacing:.7px; font-size: 20px;">Coluna DI Saturada 100</p>', unsafe_allow_html=True)
            __spaces(2)
            st.write('<p style="font-family:Roboto:wght@100; letter-spacing:.7px; font-size: 20px;">Coluna DI Saturada 101</p>', unsafe_allow_html=True)
            __spaces(2)
            st.write('<p style="font-family:Roboto:wght@100; letter-spacing:.7px; font-size: 20px;">Necessário Regenerar 100</p>', unsafe_allow_html=True)
            __spaces(2)
            st.write('<p style="font-family:Roboto:wght@100; letter-spacing:.7px; font-size: 20px;">Necessário Regenerar 101</p>', unsafe_allow_html=True)

        with col_eta_check_sim:      
            __spaces(7)   
            st.checkbox(label="Sim", value=False, key="troca_filtro_polidor_1_sim")
            __spaces(3) 
            st.checkbox(label="Sim", value=False, key="troca_filtro_polidor_2_sim")
            __spaces(2) 
            st.checkbox(label="Sim", value=False, key="coluna_di_saturada_100_sim")
            __spaces(3) 
            st.checkbox(label="Sim", value=False, key="coluna_di_saturada_101_sim")
            __spaces(2) 
            st.checkbox(label="Sim", value=False, key="regenerar_100_sim")
            __spaces(3) 
            st.checkbox(label="Sim", value=False, key="regenerar_101_sim")

        with col_eta_check_nao:
            __spaces(7)   
            st.checkbox(label="Não", value=False, key="troca_filtro_polidor_1_nao")
            __spaces(3) 
            st.checkbox(label="Não", value=False, key="troca_filtro_polidor_2_nao")
            __spaces(2)
            st.checkbox(label="Não", value=False, key="coluna_di_saturada_100_nao")
            __spaces(3)
            st.checkbox(label="Não", value=False, key="coluna_di_saturada_101_nao")
            __spaces(2)
            st.checkbox(label="Não", value=False, key="regenerar_100_nao")
            __spaces(3)
            st.checkbox(label="Não", value=False, key="regenerar_101_nao")

        with col_empty1:
            st.empty()


        # Coluna da ETEI
        with col_etei:
            st.header("ETEI")
            __spaces(2)
            st.write('<p style="font-family:Roboto:wght@100; letter-spacing:.7px; font-size: 20px;">Dosou Antiespumante MBR</p>', unsafe_allow_html=True)
            __spaces(2)
            st.write('<p style="font-family:Roboto:wght@100; letter-spacing:.7px; font-size: 20px;">Envio Sanitário MBR</p>', unsafe_allow_html=True)
            __spaces(2)
            st.write('<p style="font-family:Roboto:wght@100; letter-spacing:.7px; font-size: 20px;">Transbordou MBR</p>', unsafe_allow_html=True)
            __spaces(2)
            st.write('<p style="font-family:Roboto:wght@100; letter-spacing:.7px; font-size: 20px;">Troca do Filtro Polidor</p>', unsafe_allow_html=True)
            __spaces(2)
            st.write('<p style="font-family:Roboto:wght@100; letter-spacing:.7px; font-size: 20px;">Houve Quebra de Emulsão</p>', unsafe_allow_html=True)
            __spaces(2)
            st.write('<p style="font-family:Roboto:wght@100; letter-spacing:.7px; font-size: 20px;">Nível do Silo de Cal (%)</p>', unsafe_allow_html=True)

        with col_etei_check_sim:      
            __spaces(7)   
            st.checkbox(label="Sim", value=False, key="dosou_antiespumante_mbr_sim")
            __spaces(3) 
            st.checkbox(label="Sim", value=False, key="envio_sanitario_mbr_sim")
            __spaces(2) 
            st.checkbox(label="Sim", value=False, key="transbordou_mbr_sim")
            __spaces(3) 
            st.checkbox(label="Sim", value=False, key="troca_filtro_polidor_sim")
            __spaces(2) 
            st.checkbox(label="Sim", value=False, key="quebra_emulsao_sim")

            st.text_input(label="", placeholder="0 - 100", key="silo_cal")

        with col_etei_check_nao:      
            __spaces(7)   
            st.checkbox(label="Não", value=False, key="dosou_antiespumante_mbr_nao")
            __spaces(3) 
            st.checkbox(label="Não", value=False, key="envio_sanitario_mbr_nao")
            __spaces(2) 
            st.checkbox(label="Não", value=False, key="transbordou_mbr_nao")
            __spaces(3) 
            st.checkbox(label="Não", value=False, key="troca_filtro_polidor_nao")
            __spaces(2) 
            st.checkbox(label="Não", value=False, key="quebra_emulsao_nao")


        # Observações
        __spaces(2)
        st.subheader("Observações")        
        
        col_obs_1, col_empty3, col_obs_2 = st.columns([6,1,6])

        # Coluna 1 Observações
        with col_obs_1:
            st.text_area("Gerais", placeholder="Observações Gerais", key="obs_geral")
            __spaces(1)
            st.text_area("ETA / ETEI", placeholder="Observações ETA / ETEI", key="obs_eta_etei")
            __spaces(1)
            st.text_area("Produtos Químicos", placeholder="Observações Químicos", key="obs_quim")

        with col_empty3:
            st.empty()

        # Coluna 2 Observações
        with col_obs_2:
            st.text_area("MBR / Aeração / Sanitária", placeholder="Observações MBR / Aeração / Sanitária", key="obs_mbr_aeracao_sanitaria")
            __spaces(1)
            st.text_area("Utilidades", placeholder="Observações Utilidades", key="obs_utilidades")            
            __spaces(1)
            st.text_area("Scrap / Bulk System", placeholder="Observações Scrap / Bulk System", key="obs_scrap_bulk")

        __spaces(2)
        # Botão de Envio do Forms para o BD
        st.form_submit_button(label="Enviar", on_click=__submit_callback)





def __buscar_dados() -> None:
    """Estrutura no menu lateral para definir o filtro de busca por data e turno.
    """
    
    # Menu lateral para busca
    st.sidebar.write("")
    st.sidebar.write("\n\nFaça sua busca:\n\n")
    st.sidebar.date_input("Data", key="date_search")
    st.sidebar.selectbox(label="Turno", options=["Selecione", "A", "B", "C"], key="sft_search")
    st.sidebar.button(label="Buscar", key="search_button")

    # Search button click
    if st.session_state.search_button:
        __search_callback()





def main() -> None:
    """Função principal para guia de execuções na aplicação.
    """

    st.title("Diário de Turno - Meio Ambiente")

    # Side menu
    menu = ['Home', 'Inserir', 'Buscar']
    choice = st.sidebar.selectbox("Menu", menu)

    if choice == "Home":
        __search_callback(home=True)
    elif choice == "Inserir":
        __inserir_dados()
    elif choice == "Buscar":
        __buscar_dados()





## EXECUÇÃO PRINCIPAL
if __name__ == "__main__":
    main()
