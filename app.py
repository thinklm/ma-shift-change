## IMPORTS
from google.cloud import firestore
from google.cloud.firestore_v1.query import Query
import json, re
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
    st.write(f"Erro na conexão com o Banco de Dados:\n{e}")
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
        query (dict): Objeto com os valores a serem apresentados na aplicação
    """
    # Verifica a ausência de registro
    if (query_eta == None) | (query_eta["date"] == ""):
        st.write("## :warning: Busca não encontrada!")

    else:
        # dia, hora e turno da última modificação
        st.subheader("Última Modificação:\n\n")
        col_data, col_hora, col_turno, col_spare = st.columns([1, 1, 1, 3])
        with col_data:
            st.write(f"Dia: {query_eta['date'].astimezone(pytz.timezone('America/Sao_Paulo')).strftime('%d/%m/%Y')}")
        with col_hora:
            st.write(f"Hora: {query_eta['date'].astimezone(pytz.timezone('America/Sao_Paulo')).strftime('%H:%M:%S')}")
        with col_turno:
            st.write(f"Turno: {query_eta['endedshift']}")
        with col_spare:
            st.empty()

        # Detalhes do turno selecinado
        col_eta, col_spare, col_etei, col_spare_2, col_obs = st.columns([4,1,4,1,5])

        with col_eta:
            st.write("## ETA")
            # Itens
            __spaces(1)
            sign = ":white_check_mark:" if query_eta["coluna_di_saturada_100"] else ":x:"
            st.write(f"{sign} Coluna DI Saturada 100")

            __spaces(1)
            sign = ":white_check_mark:" if query_eta["coluna_di_saturada_101"] else ":x:"
            st.write(f"{sign} Coluna DI Saturada 101")

            __spaces(1)
            sign = ":white_check_mark:" if query_eta["troca_filtro_polidor_1"] else ":x:"
            st.write(f"{sign} Troca Filtro Polidor 1")

            __spaces(1)
            sign = ":white_check_mark:" if query_eta["troca_filtro_polidor_2"] else ":x:"
            st.write(f"{sign} Troca Filtro Polidor 2")

            __spaces(1)
            sign = ":white_check_mark:" if query_eta["regenerar_100"] else ":x:"
            st.write(f"{sign} Necessário Regenerar 100")

            __spaces(1)
            sign = ":white_check_mark:" if query_eta["regenerar_101"] else ":x:"
            st.write(f"{sign} Necessário Regenerar 101")


        with col_spare:
            st.empty()


        with col_etei:
            st.write("## ETEI")
            # Itens
            __spaces(1)
            sign = ":white_check_mark:" if query_etei["troca_filtro_polidor"] else ":x:"
            st.write(f"{sign} Troca do Filtro Polidor")

            __spaces(1)
            sign = ":white_check_mark:" if query_etei["dosou_antiespumante_mbr"] else ":x:"
            st.write(f"{sign} Dosou Antiespumante (MBR)")

            __spaces(1)
            sign = ":white_check_mark:" if query_etei["envio_sanitario_mbr"] else ":x:"
            st.write(f"{sign} Envio Sanitária (MBR)")

            __spaces(1)
            sign = ":white_check_mark:" if query_etei["transbordou_mbr"] else ":x:"
            st.write(f"{sign} Transbordo (MBR)")

            __spaces(1)
            sign = ":white_check_mark:" if query_etei["nivel_silo_cal"] > 30 else ":x:"
            st.write(f"{sign} Nível do Silo  de Cal: {query_etei['nivel_silo_cal']}%")


        with col_spare_2:
            st.empty()


        with col_obs:
            st.write("## Observações")

            if ((query_obs["geral"] == "") and (query_obs["eta"] == "") and (query_obs["etei"] == "")
            and (query_obs["quimicos"] == "") and (query_obs["sanitaria"] == "") and (query_obs["mbr"] == "")):
                st.write("Sem observações.")

            else:
                if query_obs["geral"] != "":
                    st.write("#### Gerais:")
                    st.write(f'> {query_obs["geral"]}')
                    __spaces(1)

                if query_obs["eta"] != "":
                    st.write("#### ETA:")
                    st.write(f'> {query_obs["eta"]}')
                    __spaces(1)
                    
                if query_obs["etei"] != "":
                    st.write("#### ETEI:")
                    st.write(f'> {query_obs["etei"]}')
                    __spaces(1)

                if query_obs["quimicos"] != "":
                    st.write("#### Qupimicos:")
                    st.write(f'> {query_obs["quimicos"]}')
                    __spaces(1)

                if query_obs["mbr"] != "":
                    st.write("#### MBR / Aeração:")
                    st.write(f'> {query_obs["mbr"]}')
                    __spaces(1)

                if query_obs["sanitaria"] != "":
                    st.write("#### Sanitária:")
                    st.write(f'> {query_obs["sanitaria"]}')
                    __spaces(1)





def _upload_shift_data(submit_args: dict, teste: bool=False) -> None:
    """Faz o upload de dados do turno selecionado para o banco de dados.

    Args:
        submit_args (dict): Objeto com os valores a serem salvos
        teste (bool, optional): Modo de teste configurável para o desenvolvedor. Defaults to False.
    """
    submit_args = __parse_to_boolean(submit_args=submit_args)
    now = datetime.now().astimezone(pytz.timezone("America/Sao_Paulo"))

    new_id = now.strftime("%d%m%Y") + st.session_state.sft + str(random.randrange(1, 10**3)).zfill(4)  
    for key in submit_args.keys():
        submit_args[key]["date"] = now

    doc_ref_eta = db.collection(u"fechamento_eta").document(new_id)
    doc_ref_eta.set(submit_args["ETA"])
    doc_ref_etei = db.collection(u"fechamento_etei").document(new_id)
    doc_ref_etei.set(submit_args["ETEI"])
    doc_ref_obs = db.collection(u"fechamento_obs").document(new_id)
    doc_ref_obs.set(submit_args["OBS"])
    




def __submit_callback() -> None:
    """Callback Function para o Submit do Forms para Inserir Dados.
    """
    
    if st.session_state.sft == "Selecione":
        st.error("Turno não selecionado!")
    elif st.session_state.silo_cal == "":
        st.error("Nível de Silo de Cal não informado!")
    else:   
        submit_args = {
            "ETA": {
                "endedshift": st.session_state.sft,
                "coluna_di_saturada_100": st.session_state.coluna_di_saturada_100,
                "coluna_di_saturada_101": st.session_state.coluna_di_saturada_101,
                "regenerar_100": st.session_state.regenerar_100,
                "regenerar_101": st.session_state.regenerar_101,
                "troca_filtro_polidor_1": st.session_state.troca_filtro_polidor_1,
                "troca_filtro_polidor_2": st.session_state.troca_filtro_polidor_2,
            },
            "ETEI": {
                "endedshift": st.session_state.sft,
                "dosou_antiespumante_mbr": st.session_state.antiespumante_mbr,
                "envio_sanitario_mbr": st.session_state.sanitario_mbr,
                "transbordou_mbr": st.session_state.transbordou_mbr,
                "troca_filtro_polidor": st.session_state.troca_filtro_polidor_etei,
                "nivel_silo_cal": float(st.session_state.silo_cal),
            },
            "OBS": {
                "endedshift": st.session_state.sft,
                "geral": st.session_state.obs_geral,
                "eta": st.session_state.obs_eta,
                "quimicos": st.session_state.obs_quim,
                "etei": st.session_state.obs_etei,
                "mbr": st.session_state.obs_mbr,
                "sanitaria": st.session_state.obs_sanitaria
            }
        }

        
        # Sobe os dados do turno para o banco de dados
        _upload_shift_data(submit_args)

        # Mensagem de sucesso
        st.success("Dados enviados com sucesso!")
        
        # Limpar os campos de se inserir dados
        clear_list_1 = ["sft", "silo_cal", "obs_geral", "obs_eta", 
        "obs_quim", "obs_etei", "obs_mbr", "obs_sanitaria"]
        clear_list_2 = ["coluna_di_saturada_100", "coluna_di_saturada_101", "regenerar_100",
        "regenerar_101", "troca_filtro_polidor_1", "troca_filtro_polidor_2", "antiespumante_mbr",
        "sanitario_mbr", "transbordou_mbr", "troca_filtro_polidor_etei"]

        for key in clear_list_1:
            if key not in st.session_state.keys():
                st.error("Chave inexistente")
            elif key == "sft":
                st.session_state[key] = "Selecione"
            else:
                st.session_state[key] = ""

        for key in clear_list_2:
            if key not in st.session_state.keys():
                st.error("Chave inexistente")
            else:
                st.session_state[key] = "Não"




def __search_callback(home: bool=False) -> None:
    """Callback Function para Buscar dados e apresentá-los na aplicação.
    Serve tanto para a Home quanto para a tela de Buscar.

    Args:
        home (bool, optional): Opção para identificar a busca específica para a Home. Defaults to False.
    """
    # Busca todos os dados da data e turno escolhidos
    query_eta = __query(area="eta", home=home)
    query_etei = __query(area="etei", home=home)
    query_obs = __query(area="obs", home=home)

    # junta todas as informações
    merged_query_eta = __merge_docs(area="eta", query=query_eta)
    merged_query_etei = __merge_docs(area="etei", query=query_etei)
    merged_query_obs = __merge_docs(area="obs", query=query_obs)

    # mostra as informações na tela
    __display_shift_info(merged_query_eta, merged_query_etei, merged_query_obs)





def __inserir_dados() -> None:
    """Estrutura de Formulário para inserir novos dados de turno para o Banco de Dados.
    """

    # Header
    st.header("Inserir Dados")

    col_shift, col_empty  = st.columns([1,5])
    with col_shift:
        st.selectbox(label="Turno: ", options=["Selecione", "A", "B", "C"], key="sft")
    with col_empty:
        st.empty()    
    
    # Forms
    with st.form(key='form_in', clear_on_submit=False):
        col_eta, col_eta_radio, col_empty1, col_mbr, col_mbr_radio, col_empty2, col_etei, col_etei_radio = st.columns([2,1,1,2,1,1,2,1])

        # Coluna da ETA
        with col_eta:
            st.header("ETA")
            __spaces(3)
            st.text("Troca do Filtro Polidor 1")
            __spaces(4)
            st.text("Troca do Filtro Polidor 2")   
            __spaces(3)
            st.text("Coluna DI Saturada 100")
            __spaces(4)
            st.text("Coluna DI Saturada 101")  
            __spaces(4)
            st.text("Necessário Regenerar 100")
            __spaces(4)
            st.text("Necessário Regenerar 101")  

        with col_eta_radio:      
            __spaces(5)   
            st.radio(label="", options=["Sim", "Não"], index=1, key="troca_filtro_polidor_1")
            st.radio(label="", options=["Sim", "Não"], index=1, key="troca_filtro_polidor_2")
            st.radio(label="", options=["Sim", "Não"], index=1, key="coluna_di_saturada_100")
            st.radio(label="", options=["Sim", "Não"], index=1, key="coluna_di_saturada_101")
            st.radio(label="", options=["Sim", "Não"], index=1, key="regenerar_100")
            st.radio(label="", options=["Sim", "Não"], index=1, key="regenerar_101")

        with col_empty1:
            st.empty()


        # Coluna do MBR
        with col_mbr:
            st.header("MBR")
            __spaces(3)
            st.text("Dosou Antiespumante")
            __spaces(4)
            st.text("Envio Sanitário MBR")   
            __spaces(3)
            st.text("Transbordou MBR") 

        with col_mbr_radio:      
            __spaces(5)   
            st.radio(label="", options=["Sim", "Não"], index=1, key="antiespumante_mbr")
            st.radio(label="", options=["Sim", "Não"], index=1, key="sanitario_mbr")
            st.radio(label="", options=["Sim", "Não"], index=1, key="transbordou_mbr")

        with col_empty2:
            st.empty()


        # Coluna da ETEI
        with col_etei:
            st.header("ETEI")
            __spaces(3)
            st.text("Troca do Filtro Polidor")
            __spaces(4)
            st.text("Nível Silo de Cal (%)")   

        with col_etei_radio:      
            __spaces(5)   
            st.radio(label="", options=["Sim", "Não"], index=1, key="troca_filtro_polidor_etei")
            st.text_input(label="", placeholder="0 - 100", key="silo_cal")



        # Observações
        __spaces(2)
        st.subheader("Observações")        
        
        col_obs_1, col_empty3, col_obs_2 = st.columns([6,1,6])

        # Coluna 1 Observações
        with col_obs_1:
            st.text_area("Geral", placeholder="Observações", key="obs_geral")

            __spaces(1)
            st.text_area("ETA", placeholder="Observações", key="obs_eta")

            __spaces(1)
            st.text_area("Produtos Químicos", placeholder="Observações", key="obs_quim")

        with col_empty3:
            st.empty()

        # Coluna 2 Observações
        with col_obs_2:
            st.text_area("ETEI", placeholder="Observações", key="obs_etei")

            __spaces(1)
            st.text_area("MBR / Aeração", placeholder="Observações", key="obs_mbr")

            __spaces(1)
            st.text_area("Sanitária", placeholder="Observações", key="obs_sanitaria")

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