#' Create a normal materialized view from the set available on the MIMIC code repository
#' This function creates a materialized view from those available on the MIMIC code repository.  See details about where these come from.
#' @param URL a character string URL to the location of a plaintext file containing sql code.
#' @param dbSource your dplyr database source (Currently must be postgres).  Defaults to pg_src in the global environment.
#' @param conn your postgres DBI object to connect to the postgres db
#' @return a remote processed view (dplyr).
#' @author Jesse D. Raffa
#' @details
#' TBD
#' @export
#' @import dplyr
#' @import DBI
#' @import stringr
#' @import RPostgreSQL
#' @examples
#' ## Setup Connection
#' # m <- dbDriver("PostgreSQL")
#' # passwd <- "secret";
#' # con <- dbConnect(m, user="user", password=passwd, dbname="mimic",host="localhost",port=5674) # The trickiest part
#' # dbSendStatement(con,"SET search_path to mimiciii"); # or whatever schema you have
#' # pg_src <- src_postgres(dbname = "mimic", host = "127.0.0.1", port = 5674, user = "user", password = passwd,options="-c search_path=mimiciii")
#' # myView <- get_view("https://raw.githubusercontent.com/MIT-LCP/mimic-code/master/concepts/code-status.sql",dbSource=pg_src,conn=con)
#'

get_view <- function(URL,dbSource,conn) {
  x_code <- paste0(readLines(URL),collapse= "\n")
  name_view <-  str_split(str_split((x_code), fixed("CREATE MATERIALIZED VIEW ",ignore_case = TRUE))[[1]][2], "\\W")[[1]][1]
  if(is.na(name_view) | is.null(name_view)) {
    name_view <- str_split(str_split((x_code), fixed("DROP MATERIALIZED VIEW IF EXISTS ",ignore_case = TRUE))[[1]][2], "\\W")[[1]][1]
    if(is.na(name_view) | is.null(name_view)) {
      name_view <- str_split(str_split((x_code), fixed("DROP TABLE IF EXISTS ",ignore_case = TRUE))[[1]][2], "\\W")[[1]][1]
      }
  }
  dbSendStatement(conn,x_code)
  out <- try({ tbl(dbSource,name_view)},silent=TRUE)
  if(class(out)[1]=="try-error") {
    out <- try({ tbl(dbSource,tolower(name_view))},silent=TRUE)
    if(class(out)[1]=="try-error") {
      message(paste0("May have error on view ", name_view))
      return(out) 
    }
  }
  else {
    
  }
  return(out)
}




#' Create *ALL* materialized views from the set available on the MIMIC code repository
#' This function creates a materialized view from those available on the MIMIC code repository.  See details about where these come from.
#' @param URLlist a character string URL to the location of a plaintext file containing a list of all SQL files to create the views
#' @param dplyrDB your dplyr database source (Currently must be postgres).
#' @param special binary indicated whether to do the special case materialized views which require some additional care.  Defaults to fall
#' @param con your postgres DBI object to connect to the postgres db
#' @param specialURL a character string URL to the locate of a XML file containing the special handling instructions.
#' @return a list containing all the remote processed views.
#' @author Jesse D. Raffa
#' @details
#' TBD
#' @export
#' @import dplyr
#' @import DBI
#' @import stringr
#' @import RPostgreSQL
#' @import XML
#' @examples
#' ## Setup Connection
#' # m <- dbDriver("PostgreSQL")
#' # passwd <- "secret";
#' # con <- dbConnect(m, user="user", password=passwd, dbname="mimic",host="localhost",port=5674) # The trickiest part
#' # dbSendStatement(con,"SET search_path to mimiciii"); # or whatever schema you have
#' # pg_src <- src_postgres(dbname = "mimic", host = "127.0.0.1", port = 5674, user = "user", password = passwd,options="-c search_path=mimiciii")
#' # myViews <- get_views("https://raw.githubusercontent.com/MIT-LCP/mimic-code/master/concepts/code-status.sql",dbSource=pg_src,conn=con)
#'


get_views <- function(URLlist="london2016_datathon_cirr/mat_view_urls",con,dplyrDB,use_special=FALSE,specialURL="") {
  URLs <- readLines(URLlist)
  out <- sapply(URLs,function(x) { get_view(x,dplyrDB,con)})
  if(use_special) {
    xml <- xmlParse(paste0(readLines((url(specialURL,"rt"))),collapse = "\n"))
    #xml_by_case <- xml_find_all(xml,"//case")
    xml_by_case <- getNodeSet(xml,"//case")
    out2 <- sapply(1:length(xml_by_case), function(x) { get_special_case(xml_by_case[[x]],dplyrDB,con)})
    return(c(out,out2))
  } else {
    return(out)
  }

}


#' Process special materialized views from the set available on the MIMIC code repository
#' This function creates a materialized view from those available on the MIMIC code repository.  See details about where these come from.
#' @param xml a character string URL to the location of a XML file containing special handling instructions.
#' @param dbSource your dplyr database source (Currently must be postgres).  Defaults to pg_src in the global environment.
#' @param conn your postgres DBI object to connect to the postgres db
#' @return a remote processed view (dplyr).
#' @author Jesse D. Raffa
#' @details
#' TBD
#' @export
#' @import RPostgreSQL
#' @import DBI
#' @import XML
#' @example
#' # get_special_case("mat_view_special.xml",dbSource=pg_src,conn=con)

get_special_case <- function(xml,dbSource,conn) {
  basefile <- readLines(xmlValue(getNodeSet(xml,"//url")[[1]]))
  # apply regex
  basefile <- basefile[!stringr::str_detect(basefile,"copy")]
  x_code <- paste0(basefile,collapse= "\n");
  dbSendStatement(conn,x_code)
  extraInfo <- xmlToDataFrame(getNodeSet(xml,".//extra_file"))
  lapply(1:nrow(extraInfo),function(x) { extraAction(extraInfo[x,],conn)})


}

#' Process the actions required for special handling as instructed for a case from the set available on the MIMIC code repository
#' This function creates a materialized view from those available on the MIMIC code repository.  See details about where these come from.
#' @param x an XML node
#' @param conn your postgres DBI object to connect to the postgres db
#' @return TBD
#' @author Jesse D. Raffa
#' @details
#' TBD
#' @export
#' @importFrom utils read.csv
#' @import RPostgreSQL
#' @import DBI
#' @import XML
#' @example
#' # extraAction(x=node,conn=con)

extraAction <- function(x,conn) {
  if(x$method=="pushcsv") {
    dat <- read.csv(textConnection(readLines(gzcon(url(as.character(x$url))))),header=FALSE)
    dbSendStatement(conn,paste0("COPY ", x$target , " FROM STDIN"))
    postgresqlCopyInDataframe(conn,dataframe = dat)

  } else {
    stop("Unsupported method for special case data")
  }

}
