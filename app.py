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
    st.write(f"Erro na conexão com o Banco de Dados:\n{e}")
    st.write("Se persistir o erro, contate o desenvolvedor!")




## FUNÇÕES
def __spaces(repeticoes: int = 3) -> None:
    """Escreve espaços vazios para como forma de adicionar espaços verticais entre os elementos.

    Args:
        repeticoes (int, optional): Quantidade de espaços verticais. Defaults to 3.
    """
    _ = [st.write("") for _ in range(repeticoes)]



def __query(home: bool=False) -> Query.stream:
    """Busca no Banco de Dados com filtros.

    Args:
        home (bool, optional): Opção para identificar a busca específica para a Home. Defaults to False.

    Returns:
        Query.stream: Generator com objetos correspondentes aos filtros de busca aplicados.
    """
    fechamentos_ref = db.collection(u"fechamento_eta").document(u"JWEz12ARyHZGAs9qX3qy").collection("teste_")
    docs = fechamentos_ref.order_by(
        u"date", direction=firestore.Query.DESCENDING        
    ).limit(1)

    date_query = datetime.strptime(
        f"{docs.get()[0].to_dict()['date'].astimezone(pytz.timezone('America/Sao_Paulo')).date()}", "%Y-%m-%d"
    )
    shift = docs.get()[0].to_dict()["endedshift"]


    fechamentos_ref = db.collection(u"teste_fechamento")
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




def __merge_docs(query: Query.stream) -> dict:
    """Concatena os documentos com a mesma especificação em um só.

    Args:
        query (Query.stream): Stream de queries resultantes da busca filtrada no banco de dados

    Returns:
        dict: Objeto com as informações concatenadas. 
        Retorna None caso não haja registros para a busca aplicada.
    """

    merged = {
        "date": "",
        "endedshift": "",
        "area": "",
        "coluna_di_saturada_100": "",
        "coluna_di_saturada_101": "",
        "regenerar_100": "",
        "regenerar_101": "",
        "troca_filtro_polidor_1": "",
        "troca_filtro_polidor_1": "",
    }

    for doc in query:
        for key in merged.keys():
            
            if key == "date":
                if doc.to_dict()[key] == "":
                    return None
                merged[key] = doc.to_dict()[key].astimezone(pytz.timezone("America/Sao_Paulo"))
            
            elif (key == "endedshift") | (merged[key] == ""):
                merged[key] = doc.to_dict()[key]   
            
            else:
                merged[key] = merged[key] + "\n\n" + doc.to_dict()[key]

    return merged


def __merge_teste(query: Query.stream) -> dict:
    """Concatena os documentos com a mesma especificação em um só.

    Args:
        query (Query.stream): Stream de queries resultantes da busca filtrada no banco de dados

    Returns:
        dict: Objeto com as informações concatenadas. 
        Retorna None caso não haja registros para a busca aplicada.
    """

    merged = {
        "date": "",
        "endedshift": "",
        "coluna_di_saturada_100": "",
        "coluna_di_saturada_101": "",
        "regenerar_100": "",
        "regenerar_101": "",
        "troca_filtro_polidor_1": "",
        "troca_filtro_polidor_1": "",
    }

    for doc in query:
        for key in merged.keys():
            
            if key == "date":
                if doc.to_dict()[key] == "":
                    return None
                merged[key] = doc.to_dict()[key].astimezone(pytz.timezone("America/Sao_Paulo"))
            
            elif (key == "endedshift") | (merged[key] == ""):
                merged[key] = doc.to_dict()[key]   
            
            else:
                merged[key] = merged[key] + "\n\n" + doc.to_dict()[key]

    return merged




def __parse_to_boolean (submit_args: dict) -> dict:
    new_args = submit_args
    for key_out in new_args.keys():
        for key_in in new_args[key_out].keys():
            if new_args[key_out][key_in] == "Não":
                new_args[key_out][key_in] = False
            elif new_args[key_out][key_in] == "Sim":
                new_args[key_out][key_in] = True

    return new_args



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
                "troca_filtro_polidor_1": st.session_state.troca_filtro_polidor_2,
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




def __search_callback(home: bool) -> None:
    st.write(__merge_teste(__query()))





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
    st.write("Tela para Buscar Dados")




# def __submit_callback() -> None:
#     pass




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