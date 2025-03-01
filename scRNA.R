if (!require("BiocManager", quietly = TRUE))
  install.packages("BiocManager")

BiocManager::install("scone")
BiocManager::install("splatter")
## ----include=FALSE------------------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>"
)

## ----message=FALSE, warning=FALSE---------------------------------------------
library(SingleCellExperiment)
library(splatter)
library(scater)
library(cluster)
library(scone)

## -----------------------------------------------------------------------------
data_matrix <- read.csv("dataset.csv", header = FALSE)  
data_matrix <- as.matrix(data_matrix)  

# Aggiungere nomi predefiniti per geni e cellule (opzionale)
rownames(data_matrix) <- paste0("Gene", seq_len(nrow(data_matrix)))  # Geni: Gene1, Gene2, ...
colnames(data_matrix) <- paste0("Cell", seq_len(ncol(data_matrix)))  # Cellule: Cell1, Cell2, ...

# Creare l'oggetto SingleCellExperiment
sce <- SingleCellExperiment(assays = list(counts = data_matrix))

## -----------------------------------------------------------------------------
set.seed(1234)
assay(sce, "lograwcounts") <- log1p(counts(sce))

colData(sce)$Group <- factor(sample(c("Group1", "Group2", "Group3"), ncol(sce), replace = TRUE))



## -----------------------------------------------------------------------------
sce<-PsiNorm(sce)
sce<-logNormCounts(sce)
head(sizeFactors(sce))

## -----------------------------------------------------------------------------
set.seed(1234)
sce<-runPCA(sce, exprs_values="logcounts", scale=TRUE, name = "PsiNorm_PCA",
            ncomponents = 10)
plotReducedDim(sce, dimred = "PsiNorm_PCA", colour_by = "Group")

## -----------------------------------------------------------------------------

groups<-cluster::clara(reducedDim(sce, "PsiNorm_PCA"), k=nlevels(sce$Group))
b<-paste("ARI from PsiNorm normalized data:",
         round(mclust::adjustedRandIndex(groups$clustering, sce$Group), 
               digits = 3))

kableExtra::kable(rbind(a,b), row.names = FALSE)

## -----------------------------------------------------------------------------


# Distanza e silhouette per i dati normalizzati
dist <- daisy(reducedDim(sce, "PsiNorm_PCA"))
dist <- as.matrix(dist)
sil_norm <- silhouette(x = as.numeric(as.factor(sce$Group)), dmatrix = dist)
avg_width_norm <- mean(sil_norm[, "sil_width"])
b <- paste("Silhouette from PsiNorm normalized data:", round(avg_width_norm, digits = 3))